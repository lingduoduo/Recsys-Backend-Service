package com.demo.retrieval.evaluation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MovieLensDatasetTest {
    @TempDir
    Path tempDir;

    @Test
    void loadsRatingsAndExposesSortedImmutableViews() throws IOException {
        Path csv = csv("userId,movieId,rating,timestamp\n"
                + "2,20,4.0,1\n2,10,1.0,2\n1,20,3.0,3\n1,10,5.0,4\n");

        MovieLensDataset data = MovieLensDataset.load(csv, 2, 2);

        assertEquals(List.of("1", "2"), data.userIds());
        assertEquals(List.of("10", "20"), data.movieIds());
        assertEquals(Map.of("10", 5.0, "20", 3.0), data.ratingsFor("1"));
        assertEquals(5.0, data.rating("1", "10"));
        assertEquals(Map.of("10", 2, "20", 2), data.movieCounts());
        assertEquals(3.25, data.globalMean());
        assertThrows(UnsupportedOperationException.class, () -> data.userIds().add("3"));
        assertThrows(UnsupportedOperationException.class, () -> data.ratingsFor("1").put("30", 2.0));
        assertThrows(UnsupportedOperationException.class, () -> data.movieCounts().put("30", 1));
    }

    @Test
    void loadsOpaqueIdentifiersInNaturalStringOrder() throws IOException {
        Path csv = csv("userId,movieId,rating,timestamp\n"
                + "user_2,item_20,4.0,1\nuser_2,item_10,1.0,2\n"
                + "user_1,item_20,3.0,3\nuser_1,item_10,5.0,4\n");

        MovieLensDataset data = MovieLensDataset.load(csv, 2, 2);

        assertEquals(List.of("user_1", "user_2"), data.userIds());
        assertEquals(List.of("item_10", "item_20"), data.movieIds());
    }

    @Test
    void filtersUsersAndMoviesRepeatedlyUntilStableWithoutCollapsingCore() throws IOException {
        Path csv = csv("userId,movieId,rating\n"
                + "1,10,5\n1,20,4\n2,10,3\n2,20,2\n"
                + "3,10,1\n3,30,2\n4,30,3\n4,40,4\n");

        MovieLensDataset data = MovieLensDataset.load(csv, 2, 2);

        assertEquals(List.of("1", "2"), data.userIds());
        assertEquals(List.of("10", "20"), data.movieIds());
        assertEquals(Map.of("10", 2, "20", 2), data.movieCounts());
    }

    @Test
    void computesLeaveOneUserOutSmoothedMovieScore() throws IOException {
        Path csv = csv("userId,movieId,rating\n1,10,5\n1,20,3\n2,10,1\n2,20,4\n");
        MovieLensDataset data = MovieLensDataset.load(csv, 2, 2);

        assertEquals((1.0 + 20.0 * data.globalMean()) / 21.0,
                data.scoreExcludingUser("1", "10"), 1e-12);
    }

    @Test
    void rejectsWrongHeader() throws IOException {
        Path csv = csv("movieId,userId,rating\n10,1,5\n");
        assertThrows(IllegalArgumentException.class, () -> MovieLensDataset.load(csv, 0, 0));
    }

    @Test
    void rejectsMalformedRowsWithFileAndLineNumber() throws IOException {
        assertMalformed("userId,movieId,rating\n1,10\n", 2);
        assertMalformed("userId,movieId,rating\n,10,5\n", 2);
        assertMalformed("userId,movieId,rating\n1,,5\n", 2);
        assertMalformed("userId,movieId,rating\n1,10,NaN\n", 2);
        assertMalformed("userId,movieId,rating\n1,10,Infinity\n", 2);
    }

    @Test
    void rejectsDuplicateUserMoviePair() throws IOException {
        Path csv = csv("userId,movieId,rating\n1,10,5\n1,10,4\n");
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> MovieLensDataset.load(csv, 0, 0));
        assertTrue(error.getMessage().contains(csv.toString()));
        assertTrue(error.getMessage().contains("line 3"));
    }

    @Test
    void rejectsNegativeThresholds() throws IOException {
        Path csv = csv("userId,movieId,rating\n1,10,5\n");
        assertThrows(IllegalArgumentException.class, () -> MovieLensDataset.load(csv, -1, 0));
        assertThrows(IllegalArgumentException.class, () -> MovieLensDataset.load(csv, 0, -1));
    }

    @Test
    void rejectsEmptyPostFilterDataset() throws IOException {
        Path csv = csv("userId,movieId,rating\n1,10,5\n");
        assertThrows(IllegalArgumentException.class, () -> MovieLensDataset.load(csv, 2, 2));
    }

    @Test
    void rejectsPostFilterDatasetWithOnlyEmptyUserMaps() throws IOException {
        Path csv = csv("userId,movieId,rating\n1,10,5\n");

        assertThrows(IllegalArgumentException.class, () -> MovieLensDataset.load(csv, 0, 2));
    }

    @Test
    void removesEmptyUsersAtZeroThresholdWhileRetainingPopulatedUsers() throws IOException {
        Path csv = csv("userId,movieId,rating\n1,10,5\n"
                + "2,20,4\n2,30,3\n3,20,2\n3,30,1\n");

        MovieLensDataset data = MovieLensDataset.load(csv, 0, 2);

        assertEquals(List.of("2", "3"), data.userIds());
        assertEquals(Map.of("20", 4.0, "30", 3.0), data.ratingsFor("2"));
    }

    @Test
    void exposesImmutablePopularityOrderingWithNaturalStringTieBreaks() throws IOException {
        Path csv = csv("userId,movieId,rating\n"
                + "1,2,5\n1,10,4\n2,2,3\n2,10,2\n2,3,1\n");

        MovieLensDataset data = MovieLensDataset.load(csv, 0, 0);

        assertEquals(List.of("10", "2", "3"), data.movieIdsByPopularity());
        assertThrows(UnsupportedOperationException.class,
                () -> data.movieIdsByPopularity().add("4"));
    }

    private void assertMalformed(String content, int line) throws IOException {
        Path csv = csv(content);
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> MovieLensDataset.load(csv, 0, 0));
        assertTrue(error.getMessage().contains(csv.toString()));
        assertTrue(error.getMessage().contains("line " + line));
    }

    private Path csv(String content) throws IOException {
        Path path = tempDir.resolve("ratings-" + System.nanoTime() + ".csv");
        Files.writeString(path, content);
        return path;
    }
}
