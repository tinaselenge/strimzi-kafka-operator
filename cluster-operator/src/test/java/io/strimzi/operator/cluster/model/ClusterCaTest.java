/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.kafka.model.common.CertificateAuthorityBuilder;
import io.strimzi.certs.OpenSslCertIssuer;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.CaConfig;
import io.strimzi.operator.common.model.InternalCa;
import io.strimzi.operator.common.model.PasswordGenerator;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ClusterCaTest {
    private final String cluster = "my-cluster";

    @Test
    public void testRemoveExpiredCertificate() {
        // simulate certificate creation at following time, with expire at 365 days later (by default)
        String instantExpected = "2022-03-23T09:00:00Z";
        Clock clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());

        InternalCa clusterCa = new InternalCa(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, new OpenSslCertIssuer(clock), new PasswordGenerator(10, "a", "a"), null, null, CaConfig.createDefault());
        clusterCa.setClock(clock);
        clusterCa.createOrUpdateStrimziManagedCa(true, false, false);
        assertThat(clusterCa.caCertData().size(), is(3));

        // ... simulated at the following time, with expire at 365 days later (by default)
        instantExpected = "2022-03-23T11:00:00Z";
        clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());

        clusterCa = new InternalCa(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, new OpenSslCertIssuer(clock), new PasswordGenerator(10, "a", "a"), buildCertSecret(clusterCa), buildKeySecret(clusterCa), CaConfig.createDefault());
        clusterCa.setClock(clock);
        // force key replacement so certificate renewal ...
        clusterCa.createOrUpdateStrimziManagedCa(true, true, false);
        assertThat(clusterCa.caCertData().size(), is(4));
        assertThat(clusterCa.caCertData().containsKey("ca-2023-03-23T09-00-00Z.crt"), is(true));

        // running a CA reconcile simulated at following time (365 days later) expecting expired certificate being removed
        instantExpected = "2023-03-23T10:00:00Z";
        clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());

        clusterCa = new InternalCa(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, new OpenSslCertIssuer(), new PasswordGenerator(10, "a", "a"), buildCertSecret(clusterCa), buildKeySecret(clusterCa), CaConfig.createDefault());
        clusterCa.setClock(clock);
        clusterCa.createOrUpdateStrimziManagedCa(true, false, false);
        assertThat(clusterCa.caCertData().size(), is(3));
        assertThat(clusterCa.caCertData().containsKey("ca-2023-03-23T09-00-00Z.crt"), is(false));
    }

    @Test
    public void testIsExpiringCertificate() {
        // simulate certificate creation at following time, with expire at 365 days later (by default) and renewal days at 30 (by default)
        String instantExpected = "2022-03-30T09:00:00Z";
        Clock clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());

        InternalCa clusterCa = new InternalCa(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, new OpenSslCertIssuer(clock), new PasswordGenerator(10, "a", "a"), null, null, CaConfig.createDefault());
        clusterCa.setClock(clock);
        clusterCa.createOrUpdateStrimziManagedCa(true, false, false);

        // check certificate expiration out of the renewal period, certificate is not expiring
        instantExpected = "2023-02-15T09:00:00Z";
        clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());
        clusterCa.setClock(clock);
        assertThat(clusterCa.isExpiring(buildCertSecret(clusterCa), InternalCa.CA_CRT), is(false));

        // check certificate expiration within the renewal period, certificate is expiring
        instantExpected = "2023-03-15T09:00:00Z";
        clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());
        clusterCa.setClock(clock);
        assertThat(clusterCa.isExpiring(buildCertSecret(clusterCa), InternalCa.CA_CRT), is(true));
    }

    @Test
    public void testRemoveOldCertificate() {
        // simulate certificate creation at following time, with expire at 365 days later (by default)
        String instantExpected = "2022-03-23T09:00:00Z";
        Clock clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());

        InternalCa clusterCa = new InternalCa(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, new OpenSslCertIssuer(clock), new PasswordGenerator(10, "a", "a"), null, null, CaConfig.createDefault());
        clusterCa.setClock(clock);
        clusterCa.createOrUpdateStrimziManagedCa(true, false, false);
        assertThat(clusterCa.caCertData().size(), is(3));

        // ... simulated at the following time, with expire at 365 days later (by default)
        instantExpected = "2022-03-23T11:00:00Z";
        clock = Clock.fixed(Instant.parse(instantExpected), Clock.systemUTC().getZone());

        clusterCa = new InternalCa(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, new OpenSslCertIssuer(clock), new PasswordGenerator(10, "a", "a"), buildCertSecret(clusterCa), buildKeySecret(clusterCa), CaConfig.createDefault());
        clusterCa.setClock(clock);
        // force key replacement so certificate renewal ...
        clusterCa.createOrUpdateStrimziManagedCa(true, true, false);
        assertThat(clusterCa.caCertData().size(), is(4));
        assertThat(clusterCa.caCertData().containsKey("ca-2023-03-23T09-00-00Z.crt"), is(true));

        clusterCa.maybeDeleteOldCerts();
        assertThat(clusterCa.caCertData().size(), is(3));
        assertThat(clusterCa.caCertData().containsKey("ca-2023-03-23T09-00-00Z.crt"), is(false));
    }

    @Test
    public void testNotRemoveOldCertificateWithCustomCa() {
        // Dummy cert valid until 2126 for testing purposes
        String newDummyCert = """
                -----BEGIN CERTIFICATE-----
                MIIDlzCCAn+gAwIBAgIUemVt2M8YnfYLntb16p2/oSFQVHEwDQYJKoZIhvcNAQEL
                BQAwWjELMAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoM
                GEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDETMBEGA1UEAwwKY2x1c3Rlci1jYTAg
                Fw0yNjAyMTYxNTI1MzhaGA8yMTI2MDEyMzE1MjUzOFowWjELMAkGA1UEBhMCQVUx
                EzARBgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoMGEludGVybmV0IFdpZGdpdHMg
                UHR5IEx0ZDETMBEGA1UEAwwKY2x1c3Rlci1jYTCCASIwDQYJKoZIhvcNAQEBBQAD
                ggEPADCCAQoCggEBALulA0Z4vXwcQw9BD1ZehrAJPVg6o9ok7WxM6vbSEHc8ptV1
                97dXy0EoIcaJKnwzbwzqBL0KDVa/ZXpiHnWN/o7eq3ZBXxTv/1SGcgwx4vDN99ui
                qwyQ+eGBjdiuJ4NbRdD5rM59SOxTvL890RELrRAEW6Cx3v0A0kJkxxKLsxpwfgOY
                KpY3B46Y+LNx41upA6sLKzWxgATAeXJK5TPtr5Es8wYPYuQR0JGYJJCv2pPABHTr
                aWGQHkdDjf+YV+VISVtD3yK5uZopKZzy3qv6mhKSP5ZOv9zVFJffHrAyMjSQMiTT
                iuGZ1bJtz/f+BPxU0JlC18ZORXN7iJLHhErM+hkCAwEAAaNTMFEwHQYDVR0OBBYE
                FOCa9ssHAazYsg4eCeXJwpVaEWbcMB8GA1UdIwQYMBaAFOCa9ssHAazYsg4eCeXJ
                wpVaEWbcMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAI7tgXgb
                PKrT/0mLDK1qeIC/kCXoeAfv0WvQ8LKlGeaHG8IwXvbZS4ZVkiMIb3dN/A+taY4M
                3GvYn5O1z0v6BJhDZKVLKQXI0zYW+8w4wjoquELyz7EHR8+8tUNAN6bWoUpo6npO
                vuP+weaGjg1FHiyxmuMmqC0WqBOl71uyzUxsV4d1iOPNosdBahONQ36BafgUT4CU
                Vl47oh7M7k/idihozfse+qBWyXR8yqdhunrn6arV0aqtJ0htfXaBNFGfESvl6qhd
                UEkhEUgk/J/NUrZfq4hmnU0MwPxodua8b8gvl58n9O3f9lbmNc6k9xbt44coZEOx
                gs5WXXBDclPQMjg=
                -----END CERTIFICATE-----
                """;
        // Dummy cert valid until 2126 for testing purposes
        String dummyCert = """
                -----BEGIN CERTIFICATE-----
                MIIDlzCCAn+gAwIBAgIULOreW0R5KZmFBnzDzzU+9Yemb2MwDQYJKoZIhvcNAQEL
                BQAwWjELMAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoM
                GEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDETMBEGA1UEAwwKY2x1c3Rlci1jYTAg
                Fw0yNjAyMTYxNTI4MzhaGA8yMTI2MDEyMzE1MjgzOFowWjELMAkGA1UEBhMCQVUx
                EzARBgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoMGEludGVybmV0IFdpZGdpdHMg
                UHR5IEx0ZDETMBEGA1UEAwwKY2x1c3Rlci1jYTCCASIwDQYJKoZIhvcNAQEBBQAD
                ggEPADCCAQoCggEBAL5mmWQZhofW12eZIMULMIkDg3vmRnUgR90w2fYER4tbFzPI
                pd0ZFkqrs/TkViV5aJcl8heXjBh3rmo18dkC03E0bfCIRSB9v9hllBpcSiX9zI6S
                lqQhlXMnbcoPCK2OPZpLlajuiRnc58GLL9tv7xpgBdUeh4sZ+KsAlgFWo7SbO+hR
                eyT0Ut/XsNDTVIVpUPuqsDLYzyVLvbupe4Esf+OUGp9WYgpbzPlQtADB7FLWAO7U
                4Ag++PR4aeqwxHwXkHR2sLyfsvFQQ6H0SNRZ7EVFVWmcmSpu0OyUtCzSViEJOwTC
                wkLL3yuazj3ItntijdxEvXoi1qW33iak4DPXeb8CAwEAAaNTMFEwHQYDVR0OBBYE
                FBGpiw0mw5DlDFWNCDymbMKN/M29MB8GA1UdIwQYMBaAFBGpiw0mw5DlDFWNCDym
                bMKN/M29MA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBADUeHezN
                4A/aBNYL23XE9wJntdV02L+NfmlvbPQewKKWxupTkLhvo6Iam25SQ7YpCvN8qWau
                u/SvI51sHmUS0uGY2SfVVVADMjGkyJLtYCqYU9QSDxlWjT4pWVKz/t+L42l6Zx+A
                OfMbtJxcZnOIk52wGerTXBOYWn/7RFU5oe1Ok/y+w+CdkXVCdOc9AgO82R1KsurH
                sKNCWwP0TQxR8J7o6ycO4yQPNs36HrWDq4O05MwArSI10sY04iTNJiwVmV1Yn4t6
                jZQE7PgOGbAPOLdqzyH+VfX8N7y5HUcyrVdtCozGHwJmFUIpY3fPAuMokZjGyOm3
                b5jRgwN6wcq20no=
                -----END CERTIFICATE-----
                """;
        Map<String, String> clusterCaCertData = new HashMap<>();
        clusterCaCertData.put(InternalCa.CA_CRT, Base64.getEncoder().encodeToString(newDummyCert.getBytes()));
        clusterCaCertData.put(InternalCa.CA_STORE, Base64.getEncoder().encodeToString("dummy-p12".getBytes()));
        clusterCaCertData.put(InternalCa.CA_STORE_PASSWORD, Base64.getEncoder().encodeToString("dummy-password".getBytes()));
        // simulate old cert still present
        clusterCaCertData.put("ca-2023-03-23T09-00-00Z.crt", Base64.getEncoder().encodeToString(dummyCert.getBytes()));

        Secret clusterCaCert = new SecretBuilder()
                .withNewMetadata()
                    .withName("my-cluster-cluster-ca-cert")
                .endMetadata()
                .withData(clusterCaCertData)
                .build();

        Map<String, String> clusterCaKeyData = new HashMap<>();
        clusterCaKeyData.put(InternalCa.CA_KEY, Base64.getEncoder().encodeToString("dummy-key".getBytes()));

        Secret clusterCaKey = new SecretBuilder()
                .withNewMetadata()
                    .withName("my-cluster-cluster-ca")
                .endMetadata()
                .withData(clusterCaKeyData)
                .build();

        InternalCa clusterCa = new InternalCa(Reconciliation.DUMMY_RECONCILIATION, Ca.CaRole.CLUSTER_CA, new OpenSslCertIssuer(), new PasswordGenerator(10, "a", "a"), clusterCaCert, clusterCaKey, new CaConfig(new CertificateAuthorityBuilder().withGenerateCertificateAuthority(false).build(), true));

        clusterCa.maybeDeleteOldCerts();

        // checking that the cluster CA related Secret was not touched by the operator
        Map<String, String> clusterCaCertDataInSecret = clusterCa.caCertData();
        assertThat(clusterCaCertDataInSecret.size(), is(4));
        assertThat(Util.decodeFromBase64(clusterCaCertDataInSecret.get(InternalCa.CA_CRT)).equals(newDummyCert), is(true));
        assertThat(Util.decodeFromBase64(clusterCaCertDataInSecret.get(InternalCa.CA_STORE)).equals("dummy-p12"), is(true));
        assertThat(Util.decodeFromBase64(clusterCaCertDataInSecret.get(InternalCa.CA_STORE_PASSWORD)).equals("dummy-password"), is(true));
        assertThat(Util.decodeFromBase64(clusterCaCertDataInSecret.get("ca-2023-03-23T09-00-00Z.crt")).equals(dummyCert), is(true));
    }

    @Test
    public void testIncludesCaChain()  {
        String cert = "CERT";
        String caChain = "CACHAIN";
        String caChain2 = "CA2CHAIN";
        String certWithChain = cert + caChain;

        assertThat(ClusterCaCertificateIssuer.includesCaChain(certWithChain.getBytes(StandardCharsets.US_ASCII), caChain.getBytes(StandardCharsets.US_ASCII)), is(true));
        assertThat(ClusterCaCertificateIssuer.includesCaChain(cert.getBytes(StandardCharsets.US_ASCII), caChain.getBytes(StandardCharsets.US_ASCII)), is(false));
        assertThat(ClusterCaCertificateIssuer.includesCaChain(certWithChain.getBytes(StandardCharsets.US_ASCII), caChain2.getBytes(StandardCharsets.US_ASCII)), is(false));
        assertThat(ClusterCaCertificateIssuer.includesCaChain(certWithChain.getBytes(StandardCharsets.US_ASCII), cert.getBytes(StandardCharsets.US_ASCII)), is(false));
        assertThat(ClusterCaCertificateIssuer.includesCaChain(null, caChain.getBytes(StandardCharsets.US_ASCII)), is(false));
        assertThat(ClusterCaCertificateIssuer.includesCaChain(cert.getBytes(StandardCharsets.US_ASCII), null), is(false));
    }

    private Secret buildCertSecret(InternalCa strimziCa) {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(AbstractModel.clusterCaCertSecretName(cluster))
                    .withAnnotations(Map.of(InternalCa.ANNO_STRIMZI_IO_CA_CERT_GENERATION, String.valueOf(strimziCa.caCertGeneration())))
                .endMetadata()
                .withData(strimziCa.caCertData())
                .build();
    }

    private Secret buildKeySecret(InternalCa strimziCa) {
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(AbstractModel.clusterCaKeySecretName(cluster))
                    .withAnnotations(Map.of(InternalCa.ANNO_STRIMZI_IO_CA_KEY_GENERATION, String.valueOf(strimziCa.caKeyGeneration())))
                .endMetadata()
                .withData(strimziCa.caKeyData())
                .build();
    }
}
