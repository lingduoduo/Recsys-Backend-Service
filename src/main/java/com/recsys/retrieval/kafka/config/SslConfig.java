package com.demo.retrieval.kafka.config;

public record SslConfig(
        String securityProtocol,
        String saslMechanism,
        String saslUsername,
        String saslPassword) {

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private String securityProtocol, saslMechanism, saslUsername, saslPassword;

        public Builder securityProtocol(String v) { securityProtocol = v; return this; }
        public Builder saslMechanism(String v)    { saslMechanism = v;    return this; }
        public Builder saslUsername(String v)     { saslUsername = v;     return this; }
        public Builder saslPassword(String v)     { saslPassword = v;     return this; }

        public SslConfig build() {
            return new SslConfig(securityProtocol, saslMechanism, saslUsername, saslPassword);
        }
    }
}
