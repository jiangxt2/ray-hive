# Compatibility

The initial package intentionally pins the only fully exercised Python client stack:

| Component | Initial baseline | Current claim |
|---|---:|---|
| Python | 3.12, 3.13 | Package and static/unit matrix |
| Ray | 2.55.1 | Exact public API and `ReadTask` signature contract |
| PyArrow | 19.0.1 | Exact decoder and Ray block contract |
| Apache Hive | 4.2.0 | Primary real-infrastructure target |
| Apache Hive | 4.0.1 | Compatibility target; no inference from 4.2.0 |
| Apache Hive | 3.1.3 | Legacy target; Hive 3.x is end-of-life |

The wheel does not declare `ray<3` or an unconstrained PyArrow dependency. A new version is admitted
only after unit, protocol, real Hive, and Ray cluster evidence exists. Runtime signature checks fail
closed when the installed Ray DeveloperAPI shape differs.

## Capability profiles

- Core: strict one-query reads, V6 columnar rowsets, native driver, `max_retries=0`, and verified
  network purity. This profile is not yet declared production-ready.
- Extended: V9 binary columns, additional drivers/authentication, and operation logs, each admitted
  by its own matrix.
- Experimental: independent queries and unverified version canaries. Experimental results cannot
  be cited as Core evidence.

The current implementation exposes native binary + NOSASL only. HTTP, SASL, Kerberos, LDAP,
CUSTOM, PyHive, Impyla, and JDBC fail with a compatibility error rather than falling back.

Hive 4.2 `FetchResults` reports `hasMoreRows=false` unconditionally and its SQL operation creates a
fresh rowset whose advisory `startRowOffset` resets to zero for each fetch. The Core cursor policy
therefore performs serialized `FETCH_NEXT` calls until the first empty columnar rowset; it does not
infer continuity or EOF from those two unreliable fields.

Complex Hive types have no native TColumn vector. Until versioned textual encodings are verified,
ARRAY, MAP, STRUCT, UNIONTYPE, interval, and UDT values fail by default. Explicit string fallback
marks the Arrow field with `hive.type` and `hive.fallback=string` metadata.

`TIMESTAMP WITH LOCAL TIME ZONE` also fails closed in the initial Core profile. Hive 4.2 serializes
the value through a textual zone representation that does not yet have lossless DST and regional
timezone conformance evidence. Explicit string fallback preserves that server representation with
the same metadata markers.
