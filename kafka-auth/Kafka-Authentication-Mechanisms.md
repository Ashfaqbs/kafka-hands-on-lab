# **Kafka Authentication Mechanisms — All of Them, Ordered by Real-World Usage**

Different companies really do wire up Kafka auth differently — it's not one standard everyone follows, it's a handful of mechanisms Kafka supports, and each org picks based on what infra/identity system they already have (Active Directory → Kerberos, cloud-managed Kafka → SASL/PLAIN with an API key, an existing OIDC provider → OAUTHBEARER, a strict PKI shop → mutual TLS). `kafka-auth/Kafka-Auth-Must-Read.md` in this same folder covers the *why* (encryption vs. authentication) informally; this doc is the complete reference — every mechanism, ordered by how often you'll actually run into it, each with a minimal Python producer snippet using [`confluent-kafka`](https://github.com/confluentinc/confluent-kafka-python) (the client with the broadest, most reliable support for every mechanism below, since it wraps `librdkafka`).

```sh
pip install confluent-kafka
```

Every snippet below is deliberately the same 5-6 lines, differing only in the config dict — that's the actual point: **the mechanism only changes how the client authenticates to the broker, not how you produce/consume messages.**

---

## **1. PLAINTEXT (no authentication) — the default, dev-only baseline**
No credentials, no encryption. Every other doc in this repo's `docker-kafka/` and `confluent-kafka/` setups defaults to this because it's zero-friction for local learning. **Never use this beyond localhost/dev** — anyone who can reach the broker's port can produce/consume anything.

```python
from confluent_kafka import Producer

p = Producer({"bootstrap.servers": "localhost:9092"})
p.produce("my-sample-topic", key="k1", value="hello")
p.flush()
```

---

## **2. SASL/PLAIN over SSL (`SASL_SSL` + `PLAIN`) — the most common real-world default**
A username and password, sent over a TLS-encrypted connection (the SSL part is what makes sending a plaintext-looking password acceptable — without it, this would be nearly as bad as no auth at all). This is the default most managed Kafka offerings hand you day one — **Confluent Cloud's standard API key/secret model is exactly this mechanism.** If you've only ever plugged an "API key" and "API secret" into a Kafka client, you've used SASL/PLAIN.

```python
from confluent_kafka import Producer

conf = {
    "bootstrap.servers": "broker1.example.com:9093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "<api-key>",
    "sasl.password": "<api-secret>",
}
p = Producer(conf)
p.produce("my-sample-topic", key="k1", value="hello")
p.flush()
```

---

## **3. SASL/SCRAM (SHA-256 / SHA-512) over SSL — self-managed clusters' password mechanism**
Also username/password, but SCRAM (Salted Challenge Response Authentication Mechanism) never sends the password itself over the wire, even under TLS — it exchanges a cryptographic proof derived from a salted hash stored on the broker. This is what most **self-hosted** Kafka clusters use when they want real username/password auth without standing up Kerberos — credentials are created via `kafka-configs.sh --alter --add-config` against the cluster itself, not an external identity system.

```python
from confluent_kafka import Producer

conf = {
    "bootstrap.servers": "broker1.internal:9093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",   # or SCRAM-SHA-256
    "sasl.username": "app-producer",
    "sasl.password": "<scram-password>",
}
p = Producer(conf)
p.produce("my-sample-topic", key="k1", value="hello")
p.flush()
```

---

## **4. Mutual TLS (mTLS / `SSL` protocol) — certificate-based, common in regulated/enterprise environments**
No username/password at all. The client presents its own certificate (from a JKS or PEM keystore) during the TLS handshake; the broker verifies it against a trusted CA, and identity *is* the certificate's Distinguished Name. This is the "why do I need a JKS file and a cacert, no SASL at all?" scenario `Kafka-Auth-Must-Read.md` in this folder already walks through in Q&A form — it's genuinely a separate mechanism from SASL, not a variant of it. Common in banks/healthcare/anywhere with an existing internal PKI team issuing certs.

```python
from confluent_kafka import Producer

conf = {
    "bootstrap.servers": "broker1.internal:9093",
    "security.protocol": "SSL",
    "ssl.ca.location": "/path/to/ca-cert.pem",
    "ssl.certificate.location": "/path/to/client-cert.pem",
    "ssl.key.location": "/path/to/client-key.pem",
    "ssl.key.password": "<key-password>",
}
p = Producer(conf)
p.produce("my-sample-topic", key="k1", value="hello")
p.flush()
```

---

## **5. SASL/GSSAPI (Kerberos) — the "older, big-enterprise" mechanism**
This is very likely what a previous, more traditional/enterprise setup was running if it felt heavier than everywhere else — Kerberos predates Kafka entirely and was the default way large enterprises (especially ones already running Hadoop/HDFS, which grew up in the same ecosystem) did authentication. The client needs a Kerberos principal and keytab file issued by the organization's KDC (Key Distribution Center, usually tied to Active Directory); there's no username/password in the client config at all — it's a ticket-based handshake.

```python
from confluent_kafka import Producer

conf = {
    "bootstrap.servers": "broker1.internal:9093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "GSSAPI",
    "sasl.kerberos.service.name": "kafka",
    "sasl.kerberos.keytab": "/etc/security/keytabs/app.keytab",
    "sasl.kerberos.principal": "app@YOUR-REALM.COM",
}
p = Producer(conf)
p.produce("my-sample-topic", key="k1", value="hello")
p.flush()
```
Kerberos setups are also the ones most likely to route through a `krb5.conf` file the OS/JVM reads separately from the Kafka client config — if this mechanism ever "just doesn't work," that file (realm, KDC address) is usually where the problem actually is, not the Kafka config itself.

---

## **6. SASL/OAUTHBEARER (OAuth2 / OIDC) — the modern, increasingly common mechanism**
The newest mainstream mechanism, and the one you're most likely to hit if a company has standardized on an identity provider (Okta, Azure AD, a custom OIDC server) and wants Kafka to plug into it rather than maintain its own separate credential store. The client fetches a short-lived bearer token from the OAuth provider (client-credentials grant is typical for service-to-service auth) and hands it to Kafka instead of a static password — this is genuinely the "different auth again" pattern you'd notice moving to a company doing modern cloud-native identity.

```python
from confluent_kafka import Producer
import requests, time

def get_token(oauth_config):
    resp = requests.post(
        oauth_config["token_url"],
        data={"grant_type": "client_credentials"},
        auth=(oauth_config["client_id"], oauth_config["client_secret"]),
    )
    token = resp.json()
    return token["access_token"], time.time() + token["expires_in"]

conf = {
    "bootstrap.servers": "broker1.internal:9093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "OAUTHBEARER",
    "oauth_cb": lambda oauth_config: get_token({
        "token_url": "https://your-idp.example.com/oauth/token",
        "client_id": "<client-id>",
        "client_secret": "<client-secret>",
    }),
}
p = Producer(conf)
p.produce("my-sample-topic", key="k1", value="hello")
p.flush()
```
Unlike the other mechanisms, `sasl.username`/`sasl.password` don't apply here at all — `oauth_cb` is a callback the client library re-invokes whenever the token needs refreshing, since bearer tokens are deliberately short-lived (typically minutes, not the "set once" static credentials of PLAIN/SCRAM).

---

## **7. Delegation Tokens — the lightweight, short-lived option (less common in app code)**
A special case built *on top of* SASL/SCRAM: an already-authenticated client (via Kerberos, SCRAM, or mTLS) can ask the broker for a short-lived delegation token, then hand that token to *other* clients/jobs that shouldn't need direct access to the primary credential — common for Spark/Flink jobs launched on shared infrastructure where distributing a real keytab or SCRAM password to every job would be a bigger blast radius if leaked. You won't usually write this by hand as an app developer; it's created via `kafka-delegation-tokens.sh` or the AdminClient API, then used exactly like SCRAM auth:
```python
conf = {
    "bootstrap.servers": "broker1.internal:9093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "<delegation-token-id>",
    "sasl.password": "<delegation-token-hmac>",
}
```

---

## **Quick Comparison**

| Mechanism | Needs | Typical setting |
|---|---|---|
| PLAINTEXT | Nothing | Local dev only |
| SASL/PLAIN | Username + password (over TLS) | Cloud-managed Kafka (Confluent Cloud, etc.) |
| SASL/SCRAM | Username + salted-hash password (over TLS) | Self-managed clusters wanting real password auth |
| mTLS (`SSL`) | Client cert + key + CA trust | Regulated enterprises with an internal PKI |
| SASL/GSSAPI | Kerberos keytab + principal + KDC | Large/legacy enterprises, Hadoop-ecosystem shops |
| SASL/OAUTHBEARER | OAuth2/OIDC client credentials | Companies standardized on cloud IAM/OIDC |
| Delegation Tokens | A short-lived token issued by an already-authenticated client | Shared job infrastructure (Spark/Flink), not typically hand-written client code |

Authentication (*who are you*) is a separate concern from **authorization** (*what are you allowed to do*, via Kafka ACLs) — every mechanism above just answers the first question. A fully authenticated SASL/SCRAM client with no ACL grants still can't produce or consume anything.
