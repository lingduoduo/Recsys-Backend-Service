package com.recsys.retrieval.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ConfigurationProperties(prefix = "recsys.retrieval")
@Validated
public class RecommendationProperties {
    private Cache cache = new Cache();
    private Embeddings embeddings = new Embeddings();
    private CandidateGeneration candidateGeneration = new CandidateGeneration();
    private Filtering filtering = new Filtering();
    private Bandit bandit = new Bandit();
    private ReplayBuffer replayBuffer = new ReplayBuffer();
    private RewardModel rewardModel = new RewardModel();
    private Sequence sequence = new Sequence();
    private Grpo grpo = new Grpo();
    @Valid
    private ProfileAudit profileAudit = new ProfileAudit();
    @Valid
    private Measurements measurements = new Measurements();
    private Map<String, MovieProfile> catalog = new LinkedHashMap<>();
    // Optional path to a catalog JSON file ({itemId: MovieProfile}). When set, its entries are
    // merged on top of the inline `catalog` at startup so the catalog can outgrow application.yml
    // and stay in sync with the Python pipeline's catalog. Empty = use inline catalog only.
    private String catalogPath = "";

    public Embeddings getEmbeddings() {
        return embeddings;
    }

    public void setEmbeddings(Embeddings embeddings) {
        this.embeddings = embeddings;
    }

    public CandidateGeneration getCandidateGeneration() {
        return candidateGeneration;
    }

    public void setCandidateGeneration(CandidateGeneration candidateGeneration) {
        this.candidateGeneration = candidateGeneration;
    }

    public Filtering getFiltering() {
        return filtering;
    }

    public void setFiltering(Filtering filtering) {
        this.filtering = filtering;
    }

    public Bandit getBandit() {
        return bandit;
    }

    public void setBandit(Bandit bandit) {
        this.bandit = bandit;
    }

    public ReplayBuffer getReplayBuffer() {
        return replayBuffer;
    }

    public void setReplayBuffer(ReplayBuffer replayBuffer) {
        this.replayBuffer = replayBuffer;
    }

    public RewardModel getRewardModel() {
        return rewardModel;
    }

    public void setRewardModel(RewardModel rewardModel) {
        this.rewardModel = rewardModel;
    }

    public Sequence getSequence() {
        return sequence;
    }

    public void setSequence(Sequence sequence) {
        this.sequence = sequence;
    }

    public Grpo getGrpo() {
        return grpo;
    }

    public void setGrpo(Grpo grpo) {
        this.grpo = grpo;
    }

    public ProfileAudit getProfileAudit() {
        return profileAudit;
    }

    public void setProfileAudit(ProfileAudit profileAudit) {
        this.profileAudit = profileAudit;
    }

    public Measurements getMeasurements() {
        return measurements;
    }

    public void setMeasurements(Measurements measurements) {
        this.measurements = measurements;
    }

    public Cache getCache() {
        return cache;
    }

    public void setCache(Cache cache) {
        this.cache = cache;
    }

    public Map<String, MovieProfile> getCatalog() {
        return catalog;
    }

    public void setCatalog(Map<String, MovieProfile> catalog) {
        this.catalog = catalog;
    }

    public String getCatalogPath() {
        return catalogPath;
    }

    public void setCatalogPath(String catalogPath) {
        this.catalogPath = catalogPath;
    }

    public static class Cache {
        private int itemVectorMaxSize = 10_000;
        private long itemVectorTtlSeconds = 300;
        private int rewardMaxSize = 50_000;
        private long rewardTtlSeconds = 30;
        private long grpoWeightsTtlSeconds = 10;

        public int getItemVectorMaxSize() { return itemVectorMaxSize; }
        public void setItemVectorMaxSize(int itemVectorMaxSize) { this.itemVectorMaxSize = itemVectorMaxSize; }
        public long getItemVectorTtlSeconds() { return itemVectorTtlSeconds; }
        public void setItemVectorTtlSeconds(long itemVectorTtlSeconds) { this.itemVectorTtlSeconds = itemVectorTtlSeconds; }
        public int getRewardMaxSize() { return rewardMaxSize; }
        public void setRewardMaxSize(int rewardMaxSize) { this.rewardMaxSize = rewardMaxSize; }
        public long getRewardTtlSeconds() { return rewardTtlSeconds; }
        public void setRewardTtlSeconds(long rewardTtlSeconds) { this.rewardTtlSeconds = rewardTtlSeconds; }
        public long getGrpoWeightsTtlSeconds() { return grpoWeightsTtlSeconds; }
        public void setGrpoWeightsTtlSeconds(long grpoWeightsTtlSeconds) { this.grpoWeightsTtlSeconds = grpoWeightsTtlSeconds; }
    }

    public static class Embeddings {
        private String itemPrefix = "i2vEmb";
        private String userPrefix = "uEmb";

        public String getItemPrefix() {
            return itemPrefix;
        }

        public void setItemPrefix(String itemPrefix) {
            this.itemPrefix = itemPrefix;
        }

        public String getUserPrefix() {
            return userPrefix;
        }

        public void setUserPrefix(String userPrefix) {
            this.userPrefix = userPrefix;
        }
    }

    public static class CandidateGeneration {
        private int popularityFetchMultiplier = 5;
        private int coldStartPoolSize = 25;
        private int candidatePoolSize = 0; // 0 = auto: limit × popularityFetchMultiplier

        public int getPopularityFetchMultiplier() {
            return popularityFetchMultiplier;
        }

        public void setPopularityFetchMultiplier(int popularityFetchMultiplier) {
            this.popularityFetchMultiplier = popularityFetchMultiplier;
        }

        public int getColdStartPoolSize() {
            return coldStartPoolSize;
        }

        public void setColdStartPoolSize(int coldStartPoolSize) {
            this.coldStartPoolSize = coldStartPoolSize;
        }

        public int getCandidatePoolSize() {
            return candidatePoolSize;
        }

        public void setCandidatePoolSize(int candidatePoolSize) {
            this.candidatePoolSize = candidatePoolSize;
        }
    }

    public static class Filtering {
        private boolean enabled = true;
        private List<String> mutedProductTypes = new ArrayList<>();
        private List<String> mutedGenres = new ArrayList<>();
        private List<String> mutedKeywords = new ArrayList<>();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public List<String> getMutedProductTypes() {
            return mutedProductTypes;
        }

        public void setMutedProductTypes(List<String> mutedProductTypes) {
            this.mutedProductTypes = mutedProductTypes;
        }

        public List<String> getMutedGenres() {
            return mutedGenres;
        }

        public void setMutedGenres(List<String> mutedGenres) {
            this.mutedGenres = mutedGenres;
        }

        public List<String> getMutedKeywords() {
            return mutedKeywords;
        }

        public void setMutedKeywords(List<String> mutedKeywords) {
            this.mutedKeywords = mutedKeywords;
        }

    }

    public static class Bandit {
        private String algorithm = "ucb";
        private double explorationAlpha = 0.75;
        private double maxExplorationBonus = 0.25;
        private int coldStartExposureThreshold = 5;
        private double coldStartBoost = 1.35;
        private double relevanceWeight = 0.6;
        private double contentWeight = 0.25;
        private double popularityWeight = 0.15;
        private double qLearningAlpha = 0.1;
        private double qLearningGamma = 0.9;
        private double qLearningEpsilon = 0.1;

        public String getAlgorithm() {
            return algorithm;
        }

        public void setAlgorithm(String algorithm) {
            this.algorithm = algorithm;
        }

        public double getExplorationAlpha() {
            return explorationAlpha;
        }

        public void setExplorationAlpha(double explorationAlpha) {
            this.explorationAlpha = explorationAlpha;
        }

        public double getMaxExplorationBonus() {
            return maxExplorationBonus;
        }

        public void setMaxExplorationBonus(double maxExplorationBonus) {
            this.maxExplorationBonus = maxExplorationBonus;
        }

        public int getColdStartExposureThreshold() {
            return coldStartExposureThreshold;
        }

        public void setColdStartExposureThreshold(int coldStartExposureThreshold) {
            this.coldStartExposureThreshold = coldStartExposureThreshold;
        }

        public double getColdStartBoost() {
            return coldStartBoost;
        }

        public void setColdStartBoost(double coldStartBoost) {
            this.coldStartBoost = coldStartBoost;
        }

        public double getRelevanceWeight() {
            return relevanceWeight;
        }

        public void setRelevanceWeight(double relevanceWeight) {
            this.relevanceWeight = relevanceWeight;
        }

        public double getContentWeight() {
            return contentWeight;
        }

        public void setContentWeight(double contentWeight) {
            this.contentWeight = contentWeight;
        }

        public double getPopularityWeight() {
            return popularityWeight;
        }

        public void setPopularityWeight(double popularityWeight) {
            this.popularityWeight = popularityWeight;
        }

        public double getQLearningAlpha() {
            return qLearningAlpha;
        }

        public void setQLearningAlpha(double qLearningAlpha) {
            this.qLearningAlpha = qLearningAlpha;
        }

        public double getQLearningGamma() {
            return qLearningGamma;
        }

        public void setQLearningGamma(double qLearningGamma) {
            this.qLearningGamma = qLearningGamma;
        }

        public double getQLearningEpsilon() {
            return qLearningEpsilon;
        }

        public void setQLearningEpsilon(double qLearningEpsilon) {
            this.qLearningEpsilon = qLearningEpsilon;
        }
    }

    public static class ReplayBuffer {
        private int maxSize = 10000;
        private int candidateSnapshotSize = 20;
        private Duration pendingTtl = Duration.ofHours(1);

        public Duration getPendingTtl() {
            return pendingTtl;
        }

        public void setPendingTtl(Duration pendingTtl) {
            this.pendingTtl = pendingTtl;
        }

        public int getMaxSize() {
            return maxSize;
        }

        public void setMaxSize(int maxSize) {
            this.maxSize = maxSize;
        }

        public int getCandidateSnapshotSize() {
            return candidateSnapshotSize;
        }

        public void setCandidateSnapshotSize(int candidateSnapshotSize) {
            this.candidateSnapshotSize = candidateSnapshotSize;
        }
    }

    public static class RewardModel {
        private double weight = 0.25;
        private double globalWeight = 0.15;
        private double itemWeight = 0.45;
        private double genreWeight = 0.25;
        private double tagWeight = 0.15;
        private int minFeatureCount = 3;

        public double getWeight() {
            return weight;
        }

        public void setWeight(double weight) {
            this.weight = weight;
        }

        public double getGlobalWeight() {
            return globalWeight;
        }

        public void setGlobalWeight(double globalWeight) {
            this.globalWeight = globalWeight;
        }

        public double getItemWeight() {
            return itemWeight;
        }

        public void setItemWeight(double itemWeight) {
            this.itemWeight = itemWeight;
        }

        public double getGenreWeight() {
            return genreWeight;
        }

        public void setGenreWeight(double genreWeight) {
            this.genreWeight = genreWeight;
        }

        public double getTagWeight() {
            return tagWeight;
        }

        public void setTagWeight(double tagWeight) {
            this.tagWeight = tagWeight;
        }

        public int getMinFeatureCount() {
            return minFeatureCount;
        }

        public void setMinFeatureCount(int minFeatureCount) {
            this.minFeatureCount = minFeatureCount;
        }
    }

    public static class Sequence {
        /** off | shadow | on — the rating sequence rollout. */
        private String mode = "off";
        /**
         * off | shadow | on — the behavior sequence rollout, deliberately separate from
         * {@code mode}. The two read different sequences into different query fields and carry
         * different risk, so advancing one must not silently advance the other.
         */
        private String behaviorMode = "off";
        private int lookbackDays = 90;
        private int bucketFetchChunk = 7;

        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }

        public String getBehaviorMode() {
            return behaviorMode;
        }

        public void setBehaviorMode(String behaviorMode) {
            this.behaviorMode = behaviorMode;
        }

        public int getLookbackDays() {
            return lookbackDays;
        }

        public void setLookbackDays(int lookbackDays) {
            this.lookbackDays = lookbackDays;
        }

        public int getBucketFetchChunk() {
            return bucketFetchChunk;
        }

        public void setBucketFetchChunk(int bucketFetchChunk) {
            this.bucketFetchChunk = bucketFetchChunk;
        }
    }

    /** GET /api/v1/retrieval/profile-audit: bounds on how much work one call may do. */
    public static class ProfileAudit {
        /** SCAN pattern for level 0; the user id is the second ':'-separated segment. */
        @NotBlank
        private String userKeyPattern = "user:*:features";
        @Min(1)
        private int maxUsers = 10000;
        @Min(1)
        private int chunkSize = 500;
        /** Must stay well below spring.data.redis.lettuce.pool.max-active (32) so an audit cannot starve serving. */
        @Min(1)
        private int parallelism = 4;
        @Min(0)
        private int sampleItems = 5;

        public String getUserKeyPattern() {
            return userKeyPattern;
        }

        public void setUserKeyPattern(String userKeyPattern) {
            this.userKeyPattern = userKeyPattern;
        }

        public int getMaxUsers() {
            return maxUsers;
        }

        public void setMaxUsers(int maxUsers) {
            this.maxUsers = maxUsers;
        }

        public int getChunkSize() {
            return chunkSize;
        }

        public void setChunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
        }

        public int getParallelism() {
            return parallelism;
        }

        public void setParallelism(int parallelism) {
            this.parallelism = parallelism;
        }

        public int getSampleItems() {
            return sampleItems;
        }

        public void setSampleItems(int sampleItems) {
            this.sampleItems = sampleItems;
        }
    }

    public static class Grpo {
        /** off | shadow | on — the GRPO policy rollout, mirroring the sequence switches. */
        private String mode = "off";
        /**
         * Whether serving publishes its own impression events to Kafka. Off by default: it is the
         * first time this service writes to the event stream, and it is a training-data feature,
         * not a serving one.
         */
        private boolean emitEvents = false;

        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }

        public boolean isEmitEvents() {
            return emitEvents;
        }

        public void setEmitEvents(boolean emitEvents) {
            this.emitEvents = emitEvents;
        }
    }

    public static class Measurements {
        @Min(1)
        private int fairnessMinSupport = 100;
        @Min(1)
        private int freshnessWindowDays = 30;
        @DecimalMin("0.0")
        @DecimalMax("1.0")
        private double longTailPercentile = 0.80;
        @NotBlank
        @Pattern(regexp = "[a-zA-Z0-9._-]{1,64}")
        private String safetyPolicyVersion = "catalog-filter-v1";
        @Size(min = 1, max = 20)
        private List<@Positive Long> latencyBucketsMs =
            new ArrayList<>(List.of(5L, 10L, 25L, 50L, 100L, 250L, 500L, 1000L, 2500L));
        @Min(1)
        @Max(3600)
        private int throughputWindowSeconds = 60;
        @Min(1)
        @Max(3600)
        private int percentileWindowSeconds = 300;

        public int getThroughputWindowSeconds() {
            return throughputWindowSeconds;
        }

        public void setThroughputWindowSeconds(int throughputWindowSeconds) {
            this.throughputWindowSeconds = throughputWindowSeconds;
        }

        public int getPercentileWindowSeconds() {
            return percentileWindowSeconds;
        }

        public void setPercentileWindowSeconds(int percentileWindowSeconds) {
            this.percentileWindowSeconds = percentileWindowSeconds;
        }

        public int getFairnessMinSupport() {
            return fairnessMinSupport;
        }

        public void setFairnessMinSupport(int fairnessMinSupport) {
            this.fairnessMinSupport = fairnessMinSupport;
        }

        public int getFreshnessWindowDays() {
            return freshnessWindowDays;
        }

        public void setFreshnessWindowDays(int freshnessWindowDays) {
            this.freshnessWindowDays = freshnessWindowDays;
        }

        public double getLongTailPercentile() {
            return longTailPercentile;
        }

        public void setLongTailPercentile(double longTailPercentile) {
            this.longTailPercentile = longTailPercentile;
        }

        public String getSafetyPolicyVersion() {
            return safetyPolicyVersion;
        }

        public void setSafetyPolicyVersion(String safetyPolicyVersion) {
            this.safetyPolicyVersion = safetyPolicyVersion;
        }

        public List<Long> getLatencyBucketsMs() {
            return latencyBucketsMs;
        }

        public void setLatencyBucketsMs(List<Long> latencyBucketsMs) {
            this.latencyBucketsMs = latencyBucketsMs;
        }
    }

    public static class MovieProfile {
        private String title;
        private String productType;
        private List<String> genres = new ArrayList<>();
        private List<String> tags = new ArrayList<>();
        private List<String> keywords = new ArrayList<>();
        private boolean newRelease;
        private long expiresAtEpochMillis;

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getProductType() {
            return productType;
        }

        public void setProductType(String productType) {
            this.productType = productType;
        }

        public List<String> getGenres() {
            return genres;
        }

        public void setGenres(List<String> genres) {
            this.genres = genres;
        }

        public List<String> getTags() {
            return tags;
        }

        public void setTags(List<String> tags) {
            this.tags = tags;
        }

        public List<String> getKeywords() {
            return keywords;
        }

        public void setKeywords(List<String> keywords) {
            this.keywords = keywords;
        }

        public boolean isNewRelease() {
            return newRelease;
        }

        public void setNewRelease(boolean newRelease) {
            this.newRelease = newRelease;
        }

        public long getExpiresAtEpochMillis() {
            return expiresAtEpochMillis;
        }

        public void setExpiresAtEpochMillis(long expiresAtEpochMillis) {
            this.expiresAtEpochMillis = expiresAtEpochMillis;
        }
    }
}
