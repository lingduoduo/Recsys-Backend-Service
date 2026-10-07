package com.recsys.application.gateway;

import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.Server;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.dns.DatagramDnsQuery;
import io.netty.handler.codec.dns.DatagramDnsQueryDecoder;
import io.netty.handler.codec.dns.DatagramDnsResponse;
import io.netty.handler.codec.dns.DatagramDnsResponseEncoder;
import io.netty.handler.codec.dns.DefaultDnsRawRecord;
import io.netty.handler.codec.dns.DnsQuestion;
import io.netty.handler.codec.dns.DnsRecordType;
import io.netty.handler.codec.dns.DnsSection;
import io.netty.resolver.ResolvedAddressTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins how long the gateway's upstream clients cache a DNS answer. {@code MicroserviceGatewayServer} sets the
 * JDK's {@code networkaddress.cache.ttl=30}, but Armeria resolves with its own Netty DNS resolver, which
 * caches by the record's TTL and never reads that property. A fake DNS server answers with a 1 s TTL while
 * the JDK property says 30 s; connections recycle every second, so each new connection re-resolves and the
 * lookups must track the record. If Armeria ever honoured the JDK cache instead, there would be one lookup
 * and the docs that say "the record TTL governs" would be wrong.
 */
class UpstreamDnsTtlTest {

    private static final int RECORD_TTL_SECONDS = 1;

    private final AtomicInteger aQueries = new AtomicInteger();
    private EventLoopGroup dnsLoop;
    private Channel dns;
    private Server backend;
    private ClientFactory factory;

    @AfterEach
    void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
        if (backend != null) {
            backend.stop().join();
        }
        if (dns != null) {
            dns.close().sync();
        }
        if (dnsLoop != null) {
            dnsLoop.shutdownGracefully(0, 0, java.util.concurrent.TimeUnit.SECONDS).sync();
        }
    }

    /** Answers every A query for any name with 127.0.0.1 and {@code ttlSeconds}, counting the queries. */
    private int startDns(int ttlSeconds) throws InterruptedException {
        dnsLoop = new NioEventLoopGroup(1);
        dns = new Bootstrap().group(dnsLoop).channel(NioDatagramChannel.class)
                .handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    protected void initChannel(DatagramChannel ch) {
                        ch.pipeline().addLast(new DatagramDnsQueryDecoder(), new DatagramDnsResponseEncoder(),
                                new SimpleChannelInboundHandler<DatagramDnsQuery>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, DatagramDnsQuery query) {
                                        DnsQuestion question = query.recordAt(DnsSection.QUESTION);
                                        DatagramDnsResponse response = new DatagramDnsResponse(
                                                query.recipient(), query.sender(), query.id());
                                        response.addRecord(DnsSection.QUESTION, question);
                                        if (question.type() == DnsRecordType.A) {
                                            aQueries.incrementAndGet();
                                            response.addRecord(DnsSection.ANSWER, new DefaultDnsRawRecord(
                                                    question.name(), DnsRecordType.A, ttlSeconds,
                                                    Unpooled.wrappedBuffer(new byte[]{127, 0, 0, 1})));
                                        }
                                        ctx.writeAndFlush(response);
                                    }
                                });
                    }
                }).bind("127.0.0.1", 0).sync().channel();
        return ((InetSocketAddress) dns.localAddress()).getPort();
    }

    @Test
    void upstreamLookupsFollowTheRecordTtlNotTheJdkCacheTtl() throws Exception {
        // Exactly what MicroserviceGatewayServer.main does; it must have no effect on Armeria's resolver.
        java.security.Security.setProperty("networkaddress.cache.ttl", "30");
        int dnsPort = startDns(RECORD_TTL_SECONDS);
        backend = Server.builder().http(0).service("/work", (ctx, req) -> HttpResponse.of("ok")).build();
        backend.start().join();
        // The gateway's factory settings (connection recycling), with Armeria's default DNS resolver pointed at
        // the fake server instead of /etc/resolv.conf. Nothing else about the resolver is changed.
        factory = ClientFactory.builder()
                .maxConnectionAgeMillis(1000)
                .domainNameResolverCustomizer(b -> b
                        .serverAddresses(new InetSocketAddress("127.0.0.1", dnsPort))
                        .searchDomains()
                        .resolvedAddressTypes(ResolvedAddressTypes.IPV4_ONLY))
                .build();
        WebClient client = WebClient.builder("http://catalog-serving.recsys.internal:" + backend.activeLocalPort())
                .factory(factory).build();

        long until = System.nanoTime() + Duration.ofSeconds(6).toNanos();
        while (System.nanoTime() < until) {
            assertThat(client.get("/work").aggregate().join().status()).isEqualTo(HttpStatus.OK);
            Thread.sleep(100);
        }

        // ~one lookup per record TTL over 6 s; a 30 s cache would give exactly 1.
        assertThat(aQueries.get()).as("A queries over 6 s with a 1 s record TTL").isGreaterThanOrEqualTo(3);
    }
}
