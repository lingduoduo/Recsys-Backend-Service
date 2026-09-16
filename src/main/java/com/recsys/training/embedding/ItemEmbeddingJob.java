package com.recsys.training.embedding;

import org.apache.spark.ml.feature.BucketedRandomProjectionLSH;
import org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import com.recsys.infrastructure.redis.StreamingRedisUri;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;

import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.udf;

/**
 * Learns item embeddings from movie interaction sequences for similarity retrieval.
 * Word2Vec captures behavioral co-occurrence rather than movie-text similarity.
 */
public class ItemEmbeddingJob {
    private static final String RATINGS_RESOURCE = "/com/recsys/data/ratings.txt";
    private static final String DEFAULT_OUTPUT_PATH = "output/item_embeddings";
    private static final String DEFAULT_MASTER = "local[*]";

    public static void main(String[] args) {
        JobConfig config = JobConfig.fromArgs(args);

        SparkSession spark = SparkSession.builder()
                .appName("ItemEmbeddingJob")
                .master(config.master())
                .getOrCreate();

        try {
            spark.sparkContext().setLogLevel("WARN");

            Dataset<Row> sequenceDf = buildItemSequences(spark, config.ratingsPath(), config.minRating());
            System.out.println("=== User item sequences ===");
            sequenceDf.show(10, false);

            Word2Vec word2Vec = new Word2Vec()
                    .setInputCol("movieIds")
                    .setOutputCol("embeddings")
                    .setVectorSize(config.vectorSize())
                    .setWindowSize(config.windowSize())
                    .setMinCount(config.minCount())
                    .setMaxIter(config.maxIter())
                    .setStepSize(config.stepSize());

            Word2VecModel model = word2Vec.fit(sequenceDf);

            Dataset<Row> itemEmbeddings = model.getVectors();
            System.out.println("=== Item embeddings ===");
            itemEmbeddings.show(10, false);

            if (config.synonymMovieId() != null) {
                System.out.printf("=== Top similar items for movieId = %s ===%n", config.synonymMovieId());
                model.findSynonyms(config.synonymMovieId(), config.synonymCount()).show(false);
            }

            if (config.lshAnalysis()) {
                runLshAnalysis(spark, itemEmbeddings);
            }

            writeEmbeddings(itemEmbeddings, config.outputPath());
            System.out.printf("Wrote item embeddings to %s%n", config.outputPath());

            if (config.saveToRedis()) {
                saveToRedis(itemEmbeddings, config);
            }
        } finally {
            spark.stop();
        }
    }

    static Dataset<Row> buildItemSequences(SparkSession spark, String ratingsPath, double minRating) {
        Dataset<Row> ratings = spark.read()
                .format("csv")
                .option("header", "true")
                .load(ratingsPath)
                .select(
                        col("userId"),
                        col("movieId").cast("string").alias("movieId"),
                        col("rating").cast("double").alias("rating"),
                        col("timestamp").cast("long").alias("timestamp")
                );

        Column event = struct(col("timestamp"), col("movieId"));
        return ratings
                .where(col("rating").geq(minRating))
                .groupBy("userId")
                .agg(collect_list(event).alias("events"))
                .select(
                        col("userId"),
                        expr("transform(sort_array(events), x -> x.movieId)").alias("movieIds")
                )
                .where(size(col("movieIds")).gt(0));
    }

    // Demonstrates Spark MLlib BucketedRandomProjectionLSH on the trained vectors:
    // fits a bucket model, shows the bucket assignments, and finds approximate
    // nearest neighbors for a zero vector probe — same pattern as the reference Scala flow.
    private static void runLshAnalysis(SparkSession spark, Dataset<Row> itemEmbeddings) {
        // Word2Vec outputs a "vector" column of type Vector; rename to "emb" to match the LSH contract.
        Dataset<Row> embDf = itemEmbeddings.withColumnRenamed("vector", "emb");

        BucketedRandomProjectionLSH lsh = new BucketedRandomProjectionLSH()
                .setBucketLength(0.5)
                .setNumHashTables(3)
                .setInputCol("emb")
                .setOutputCol("bucketId");

        BucketedRandomProjectionLSHModel model = lsh.fit(embDf);
        Dataset<Row> bucketed = model.transform(embDf);

        System.out.println("=== LSH bucket assignments (word, emb, bucketId) ===");
        bucketed.show(10, false);

        // Find approximate nearest neighbors to the zero vector as a probe query.
        int dim = (int) ((Vector) itemEmbeddings.first().getAs("vector")).size();
        Vector probe = Vectors.zeros(dim);
        System.out.println("=== Approximate nearest neighbors (probe = zero vector, k=5) ===");
        model.approxNearestNeighbors(embDf, probe, 5).show(false);
    }

    private static void writeEmbeddings(Dataset<Row> itemEmbeddings, String outputPath) {
        UDF1<Vector, String> udf = ItemEmbeddingJob::vectorToString;

        Dataset<Row> output = itemEmbeddings
                .select(
                        col("word").cast("int").alias("movieId"),
                        udf(udf, DataTypes.StringType).apply(col("vector")).alias("vector")
                )
                .orderBy("movieId");

        output.coalesce(1)
                .write()
                .mode("overwrite")
                .option("header", "true")
                .csv(outputPath);
    }

    // Writes model vectors from each Spark partition directly to Redis without
    // collecting to the driver, so memory use is O(partition-size) not O(corpus-size).
    private static void saveToRedis(Dataset<Row> itemEmbeddings, JobConfig config) {
        final String host = config.redisHost();
        final int port = config.redisPort();
        final String prefix = config.redisKeyPrefix();
        final long ttl = config.redisTtl();
        // Read the credentials into serializable locals here; the URI itself is built inside the
        // lambda below, because a RedisURI captured from out here would not be serializable.
        final String username = config.redisUsername();
        final String password = config.redisPassword();
        final boolean tls = config.redisTls();

        itemEmbeddings.foreachPartition(rows -> {
            // Create the Lettuce client inside the partition lambda so nothing
            // non-serializable crosses the Spark closure boundary.
            RedisClient client = RedisClient.create(
                    StreamingRedisUri.from(host, port, username, password, tls));
            try (StatefulRedisConnection<String, String> conn = client.connect(StringCodec.UTF8)) {
                conn.setAutoFlushCommands(false);
                RedisAsyncCommands<String, String> async = conn.async();
                List<RedisFuture<?>> futures = new ArrayList<>();
                rows.forEachRemaining(row -> {
                    String key = prefix + ":" + row.getString(0);
                    String value = vectorToString((Vector) row.get(1));
                    futures.add(ttl > 0
                            ? async.set(key, value, SetArgs.Builder.ex(ttl))
                            : async.set(key, value));
                });
                conn.flushCommands();
                LettuceFutures.awaitAll(Duration.ofSeconds(30),
                        futures.toArray(new RedisFuture[0]));
            } finally {
                client.shutdown();
            }
        });

        System.out.printf("Saved item embeddings to Redis %s:%d (prefix='%s', ttl=%ds)%n",
                host, port, prefix, ttl);
    }

    private static String vectorToString(Vector vector) {
        return Arrays.stream(vector.toArray())
                .mapToObj(v -> String.format(Locale.US, "%.8f", v))
                .collect(Collectors.joining(" "));
    }

    private record JobConfig(
            String ratingsPath,
            String outputPath,
            String master,
            int vectorSize,
            int windowSize,
            int minCount,
            int maxIter,
            double stepSize,
            double minRating,
            boolean saveToRedis,
            String redisHost,
            int redisPort,
            String redisUsername,
            String redisPassword,
            boolean redisTls,
            String redisKeyPrefix,
            long redisTtl,
            String synonymMovieId,
            int synonymCount,
            boolean lshAnalysis
    ) {
        static JobConfig fromArgs(String[] args) {
            String ratingsPath = defaultRatingsPath();
            String outputPath = DEFAULT_OUTPUT_PATH;
            String master = DEFAULT_MASTER;
            int vectorSize = 16;
            int windowSize = 5;
            int minCount = 1;
            int maxIter = 10;
            double stepSize = 0.025;
            double minRating = 3.5;
            boolean saveToRedis = false;
            String redisHost = "localhost";
            int redisPort = 6379;
            // Default to the environment so a submitted job inherits the same credentials a
            // service reads, and so no password has to appear on the spark-submit command line.
            String redisUsername = System.getenv("REDIS_USERNAME");
            String redisPassword = System.getenv("REDIS_PASSWORD");
            boolean redisTls = Boolean.parseBoolean(System.getenv("REDIS_TLS"));
            String redisKeyPrefix = "i2vEmb";
            long redisTtl = 60 * 60 * 24;
            String synonymMovieId = "1";
            int synonymCount = 10;
            boolean lshAnalysis = false;

            for (String arg : args) {
                String[] kv = arg.split("=", 2);
                if (kv.length != 2) {
                    throw new IllegalArgumentException("Arguments must use --name=value syntax: " + arg);
                }
                String name = kv[0].replaceFirst("^--", "");
                String value = kv[1];
                switch (name) {
                    case "ratings"          -> ratingsPath = value;
                    case "output"           -> outputPath = value;
                    case "master"           -> master = value;
                    case "vector-size"      -> vectorSize = Integer.parseInt(value);
                    case "window-size"      -> windowSize = Integer.parseInt(value);
                    case "min-count"        -> minCount = Integer.parseInt(value);
                    case "max-iter"         -> maxIter = Integer.parseInt(value);
                    case "step-size"        -> stepSize = Double.parseDouble(value);
                    case "min-rating"       -> minRating = Double.parseDouble(value);
                    case "save-to-redis"    -> saveToRedis = Boolean.parseBoolean(value);
                    case "redis-host"       -> redisHost = value;
                    case "redis-port"       -> redisPort = Integer.parseInt(value);
                    case "redis-username"   -> redisUsername = value;
                    case "redis-password"   -> redisPassword = value;
                    case "redis-tls"        -> redisTls = Boolean.parseBoolean(value);
                    case "redis-key-prefix" -> redisKeyPrefix = value;
                    case "redis-ttl"        -> redisTtl = Long.parseLong(value);
                    case "synonym-movie-id" -> synonymMovieId = value.isBlank() ? null : value;
                    case "synonym-count"    -> synonymCount = Integer.parseInt(value);
                    case "lsh-analysis"     -> lshAnalysis = Boolean.parseBoolean(value);
                    default -> throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }

            return new JobConfig(
                    ratingsPath, outputPath, master,
                    vectorSize, windowSize, minCount, maxIter, stepSize, minRating,
                    saveToRedis, redisHost, redisPort, redisUsername, redisPassword, redisTls,
                    redisKeyPrefix, redisTtl,
                    synonymMovieId, synonymCount, lshAnalysis
            );
        }

        private static String defaultRatingsPath() {
            URL resource = ItemEmbeddingJob.class.getResource(RATINGS_RESOURCE);
            if (resource == null) {
                throw new IllegalStateException("Classpath resource not found: " + RATINGS_RESOURCE);
            }
            return resource.getPath();
        }
    }
}
