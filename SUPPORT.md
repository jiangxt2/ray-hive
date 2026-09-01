# Support policy

## Project status and ownership

`ray-hive` is an independent, community-maintained external connector. It is not part of Ray,
is not endorsed by the Ray project, and is not an Apache Hive subproject or endorsed by Apache
Hive. Ray and Apache Hive trademarks identify compatible upstream projects only.

The package is Alpha. Support is provided on a best-effort basis through this repository, without
a response-time or resolution-time service-level agreement. The current `CODEOWNERS` file names
one maintainer, so the project has a bus factor of one. Production Core or stable-graduation claims
remain blocked until ownership and release access have a documented backup.

## Supported stack and evidence

| Component | Alpha support claim | Required evidence |
| --- | --- | --- |
| Python | 3.12 and 3.13 | Locked package, static, unit, and contract CI |
| Ray | Final 2.55.1 through 2.56.x releases within `>=2.55.1,<2.57` | Unit and public-signature matrix; the release candidate also requires a multi-node Ray 2.56.1 run |
| PyArrow | 19.0.1 exactly | Decoder, Ray block, Hive, and cluster evidence |
| Thrift | 0.22.0 exactly | Native transport guard plus measured Hive and cluster runtime facts |
| Apache Hive | 4.2.0 | Required real-Hive integration suite against the pinned image |

Evidence is valid only for the exact source commit and runtime identity recorded by the CI, Hive,
and Ray cluster manifests. A nearby version, a mock, or a successful run from an older dependency
state does not extend the support claim. Hive 4.0.1 and 3.1.3 are compatibility targets, not current
supported profiles. Ray 2.57 or later, pre-releases, post-releases, and development builds are not
supported until a later compatibility change adds evidence and updates the declared dependency
window.

The implemented runtime profile is native binary transport with `NOSASL`. HTTP, SASL, Kerberos,
LDAP, CUSTOM authentication, PyHive, Impyla, and JDBC remain unsupported and fail closed.

## Public API and compatibility

The stable public surface consists of top-level `read_hive()`, top-level configuration and
credential types, top-level public exceptions, and the built-in `ray_hive.sql` predicate builder:
`col`, exact `Column` values, and the returned `Predicate` composition operations. These classes
are public construction and typing contracts, not subclassing extension points. `read_hive()` is
the only entry point that guarantees strict Ray task configuration with `max_retries=0`.

`ray_hive.datasource.HiveServer2Datasource` is a DeveloperAPI for advanced integration. It is not
exported from `ray_hive`, and direct use through `ray.data.read_datasource()` does not receive the
strict no-replay guarantee. Its constructor and methods may change in a `1.x` minor release, but
the module import path will not be removed during `1.x`.

Because the project is Alpha, a `1.x` minor release may tighten undocumented or unsafe behavior to
correct a correctness or security defect. This includes rejecting raw SQL that cannot be proven to
be one query-only statement. Removing a stable facade API, weakening query-only behavior, or
allowing a data query to replay requires an RFC-style design decision and a new major version.

Stable API removal must be announced in documentation and release notes and remain deprecated for
at least one minor release before removal in a later major release. DeveloperAPI changes are
documented in release notes but do not carry that signature-stability promise.

## Getting support

- Use a GitHub bug report for a reproducible defect in a supported stack. Include package versions,
  sanitized runtime facts, the failing operation phase, and a minimal query or schema without
  credentials or production data.
- Use a feature request for a new version, transport, authentication mechanism, or public API. A
  request is not a support claim until the required implementation and real-infrastructure evidence
  are merged.
- Report suspected vulnerabilities only through the private GitHub security advisory route in
  `SECURITY.md`.
- Deprecations and support-window changes are announced in release notes and this file.

## Release and maintenance responsibility

Project maintainers are responsible for reviewing exact-commit evidence before approving the
protected GitHub, TestPyPI, and PyPI release environments. A release must publish one immutable
artifact set; no maintainer may replace evidence or rebuild a tag from a different source state.

If the current maintainer cannot continue, further releases stop until repository administration,
PyPI ownership, trusted-publishing configuration, security-advisory access, and release duties are
transferred to an identified successor. If no successor accepts those responsibilities, the
repository and package will be marked unmaintained or archived with a final security notice rather
than continuing unsupported releases.
