package io.github.hectorvent.floci.services.msk;

import io.github.hectorvent.floci.core.common.AwsArnUtils;
import io.github.hectorvent.floci.core.common.AwsRegions;
import io.github.hectorvent.floci.services.msk.model.BrokerNodeGroupInfo;
import io.github.hectorvent.floci.services.msk.model.ClientAuthentication;
import io.github.hectorvent.floci.services.msk.model.EncryptionInfo;
import io.github.hectorvent.floci.services.msk.model.MskCluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/** The broker hostnames and ports MSK gives a cluster's TLS and IAM listeners. */
public final class MskBrokerEndpoints {

    /** A listener family: how a client connects and whether it must authenticate. */
    public enum Family {
        TLS(9094, false, false),
        SASL_IAM(9098, true, false),
        VPC_CONNECTIVITY_SASL_IAM(14000, true, true);

        private final int port;
        private final boolean sasl;
        private final boolean perBrokerPort;

        Family(int port, boolean sasl, boolean perBrokerPort) {
            this.port = port;
            this.sasl = sasl;
            this.perBrokerPort = perBrokerPort;
        }

        public boolean requiresSasl() {
            return sasl;
        }

        /** Multi-VPC listeners take a port per broker, 14001 upward; the others share one. */
        public int port(int broker) {
            return perBrokerPort ? port + broker : port;
        }
    }

    /** One broker of one listener family. */
    public record Endpoint(String clusterArn, Family family, int broker, String host, int port) {

        public String address() {
            return host + ":" + port;
        }
    }

    private static final int LABEL_LENGTH = 17;

    private MskBrokerEndpoints() {
    }

    public static List<Family> families(MskCluster cluster) {
        List<Family> families = new ArrayList<>();
        String clientBroker = Optional.ofNullable(cluster.getEncryptionInfo())
                .map(EncryptionInfo::getEncryptionInTransit)
                .map(t -> t.getClientBroker()).orElse("TLS_PLAINTEXT");
        if ("PLAINTEXT".equals(clientBroker)) {
            return families;
        }
        ClientAuthentication auth = cluster.getClientAuthentication();
        boolean iam = iamEnabled(auth);
        boolean openTls = auth == null || auth.getTls() != null || (auth.getUnauthenticated() != null
                && Boolean.TRUE.equals(auth.getUnauthenticated().getEnabled())) || (!iam && auth.getSasl() == null);
        if (openTls) {
            families.add(Family.TLS);
        }
        if (iam) {
            families.add(Family.SASL_IAM);
        }
        boolean vpcIam = Optional.ofNullable(cluster.getBrokerNodeGroupInfo())
                .map(BrokerNodeGroupInfo::getConnectivityInfo)
                .map(c -> c.getVpcConnectivity())
                .map(v -> iamEnabled(v.getClientAuthentication()))
                .orElse(false);
        if (vpcIam) {
            families.add(Family.VPC_CONNECTIVITY_SASL_IAM);
        }
        return families;
    }

    public static List<Endpoint> endpoints(MskCluster cluster, Family family) {
        List<Endpoint> endpoints = new ArrayList<>();
        int brokers = Math.max(1, cluster.getNumberOfBrokerNodes());
        for (int broker = 1; broker <= brokers; broker++) {
            String host = "b-" + broker + "." + (family == Family.VPC_CONNECTIVITY_SASL_IAM ? "iam." : "")
                    + domain(cluster);
            endpoints.add(new Endpoint(cluster.getClusterArn(), family, broker, host, family.port(broker)));
        }
        return endpoints;
    }

    public static List<Endpoint> allEndpoints(MskCluster cluster) {
        List<Endpoint> all = new ArrayList<>();
        for (Family family : families(cluster)) {
            all.addAll(endpoints(cluster, family));
        }
        return all;
    }

    /** The comma-separated string GetBootstrapBrokers returns for a family, or null when it has none. */
    public static String bootstrapString(MskCluster cluster, Family family) {
        if (!families(cluster).contains(family)) {
            return null;
        }
        return String.join(",", endpoints(cluster, family).stream().map(Endpoint::address).toList());
    }

    /** {@code <label>.<id>.c1.kafka.<region>.<dns-suffix>}, as MSK names a cluster's brokers. */
    static String domain(MskCluster cluster) {
        AwsArnUtils.Arn arn = AwsArnUtils.parse(cluster.getClusterArn());
        String label = cluster.getClusterName().toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
        label = label.length() > LABEL_LENGTH ? label.substring(0, LABEL_LENGTH) : label;
        String uuid = arn.resource().substring(arn.resource().lastIndexOf('/') + 1).replaceAll("[^a-z0-9]", "");
        String id = uuid.length() > 6 ? uuid.substring(0, 6) : uuid;
        return label + "." + id + ".c1.kafka." + arn.region() + "." + AwsRegions.dnsSuffixFor(arn.region());
    }

    private static boolean iamEnabled(ClientAuthentication auth) {
        return auth != null && auth.getSasl() != null && auth.getSasl().getIam() != null
                && Boolean.TRUE.equals(auth.getSasl().getIam().getEnabled());
    }
}
