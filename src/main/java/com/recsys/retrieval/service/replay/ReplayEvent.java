package com.demo.retrieval.service.replay;

import java.util.Map;

/** Single source of truth for the RL replay-event JSON schema (keys + type/version) and the
 *  feedback-completion logic. Serve-time ({@code MovieLensServingSideEffects}) and feedback-time
 *  ({@code HybridRecommendationService}) both build the event through these names. */
public final class ReplayEvent {

    private ReplayEvent() {
    }

    public static final String EVENT_TYPE = "rl_experience";
    public static final int SCHEMA_VERSION_VALUE = 1;

    public static final String TYPE = "type";
    public static final String SCHEMA_VERSION = "schemaVersion";
    public static final String USER = "user";
    public static final String REQUEST_ID = "requestId";
    public static final String STATE = "state";
    public static final String CANDIDATES = "candidates";
    public static final String ACTION_SPACE = "actionSpace";
    public static final String ACTION = "action";
    public static final String ACTION_POSITION = "actionPosition";
    public static final String SLATE_SIZE = "slateSize";
    public static final String POLICY = "policy";
    public static final String MODEL_PREDICTIONS = "modelPredictions";
    public static final String ESTIMATED_REWARD = "estimatedReward";
    public static final String ONLINE_SCORE = "onlineScore";
    public static final String BANDIT_SCORE = "banditScore";
    public static final String COLD_START = "coldStart";
    public static final String TIMESTAMP = "timestamp";
    public static final String CLICKED = "clicked";
    public static final String REWARD = "reward";
    public static final String FEEDBACK_TIMESTAMP = "feedbackTimestamp";
    public static final String NEXT_STATE = "nextState";

    /** Complete a (possibly serve-time pre-populated, possibly empty) event with feedback fields.
     *  The identity fields use putIfAbsent so a serve-time context is preserved, and an empty event
     *  (missing/expired pending context) still gets the minimum type/version/user/action. */
    public static void applyFeedback(Map<String, Object> event, String user, String item,
                                     boolean clicked, double reward, long feedbackTimestamp,
                                     Map<String, Object> nextState) {
        event.putIfAbsent(TYPE, EVENT_TYPE);
        event.putIfAbsent(SCHEMA_VERSION, SCHEMA_VERSION_VALUE);
        event.putIfAbsent(USER, user);
        event.putIfAbsent(ACTION, item);
        event.put(CLICKED, clicked);
        event.put(REWARD, reward);
        event.put(FEEDBACK_TIMESTAMP, feedbackTimestamp);
        event.put(NEXT_STATE, nextState);
    }
}
