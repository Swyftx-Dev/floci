package io.github.hectorvent.floci.services.msk;

import io.github.hectorvent.floci.services.msk.model.BrokerNodeGroupInfo;
import io.github.hectorvent.floci.services.msk.model.ClientAuthentication;
import io.github.hectorvent.floci.services.msk.model.ConnectivityInfo;
import io.github.hectorvent.floci.services.msk.model.EncryptionInTransit;
import io.github.hectorvent.floci.services.msk.model.EncryptionInfo;
import io.github.hectorvent.floci.services.msk.model.Iam;
import io.github.hectorvent.floci.services.msk.model.MskCluster;
import io.github.hectorvent.floci.services.msk.model.Sasl;
import io.github.hectorvent.floci.services.msk.model.Unauthenticated;
import io.github.hectorvent.floci.services.msk.model.VpcConnectivity;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.github.hectorvent.floci.services.msk.MskBrokerEndpoints.Family.SASL_IAM;
import static io.github.hectorvent.floci.services.msk.MskBrokerEndpoints.Family.TLS;
import static io.github.hectorvent.floci.services.msk.MskBrokerEndpoints.Family.VPC_CONNECTIVITY_SASL_IAM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MskBrokerEndpointsTest {

    private static final String ARN =
            "arn:aws:kafka:ap-southeast-2:000000000000:cluster/Orders_Events-Cluster/1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d-3";

    @Test
    void plaintextClusterHasNoListeners() {
        MskCluster cluster = cluster("PLAINTEXT", null);

        assertEquals(List.of(), MskBrokerEndpoints.families(cluster));
        assertNull(MskBrokerEndpoints.bootstrapString(cluster, TLS));
    }

    @Test
    void defaultClusterGetsTheTlsListener() {
        assertEquals(List.of(TLS), MskBrokerEndpoints.families(cluster(null, null)));
    }

    @Test
    void iamOnlyClusterGetsOnlyTheIamListener() {
        assertEquals(List.of(SASL_IAM), MskBrokerEndpoints.families(cluster("TLS", iam(false))));
    }

    @Test
    void unauthenticatedAndIamClusterGetsBoth() {
        assertEquals(List.of(TLS, SASL_IAM), MskBrokerEndpoints.families(cluster("TLS", iam(true))));
    }

    @Test
    void multiVpcIamAddsThePerBrokerListener() {
        MskCluster cluster = cluster("TLS", iam(false));
        VpcConnectivity vpcConnectivity = new VpcConnectivity();
        vpcConnectivity.setClientAuthentication(iam(false));
        ConnectivityInfo connectivity = new ConnectivityInfo();
        connectivity.setVpcConnectivity(vpcConnectivity);
        cluster.getBrokerNodeGroupInfo().setConnectivityInfo(connectivity);

        assertEquals(List.of(SASL_IAM, VPC_CONNECTIVITY_SASL_IAM), MskBrokerEndpoints.families(cluster));
        assertEquals("b-1.iam.orderseventsclust.1a2b3c.c1.kafka.ap-southeast-2.amazonaws.com:14001,"
                        + "b-2.iam.orderseventsclust.1a2b3c.c1.kafka.ap-southeast-2.amazonaws.com:14002",
                MskBrokerEndpoints.bootstrapString(cluster, VPC_CONNECTIVITY_SASL_IAM));
    }

    @Test
    void brokerNamesFollowMsksLabelRules() {
        MskCluster cluster = cluster("TLS", iam(false));

        assertEquals("b-1.orderseventsclust.1a2b3c.c1.kafka.ap-southeast-2.amazonaws.com:9098,"
                        + "b-2.orderseventsclust.1a2b3c.c1.kafka.ap-southeast-2.amazonaws.com:9098",
                MskBrokerEndpoints.bootstrapString(cluster, SASL_IAM));
    }

    private static MskCluster cluster(String clientBroker, ClientAuthentication auth) {
        MskCluster cluster = new MskCluster();
        cluster.setClusterArn(ARN);
        cluster.setClusterName("Orders_Events-Cluster");
        cluster.setNumberOfBrokerNodes(2);
        cluster.setBrokerNodeGroupInfo(new BrokerNodeGroupInfo());
        if (clientBroker != null) {
            EncryptionInTransit inTransit = new EncryptionInTransit();
            inTransit.setClientBroker(clientBroker);
            EncryptionInfo encryption = new EncryptionInfo();
            encryption.setEncryptionInTransit(inTransit);
            cluster.setEncryptionInfo(encryption);
        }
        cluster.setClientAuthentication(auth);
        return cluster;
    }

    private static ClientAuthentication iam(boolean unauthenticatedToo) {
        Iam iam = new Iam();
        iam.setEnabled(true);
        Sasl sasl = new Sasl();
        sasl.setIam(iam);
        ClientAuthentication auth = new ClientAuthentication();
        auth.setSasl(sasl);
        if (unauthenticatedToo) {
            Unauthenticated unauthenticated = new Unauthenticated();
            unauthenticated.setEnabled(true);
            auth.setUnauthenticated(unauthenticated);
        }
        return auth;
    }
}
