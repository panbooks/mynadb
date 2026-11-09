# Dependency license inventory — initial review

Date: 2026-11-09. Source: `d603624`, committed Cargo.lock, Cargo metadata
filtered for `x86_64-unknown-linux-gnu`. See
[`dependency-licenses.csv`](dependency-licenses.csv) for 602 resolved packages,
including the workspace and development dependencies. This is a metadata
inventory, not a completed attribution bundle or a legal compatibility finding.

## Findings

- The root package has no `license` or `license-file`, and no root LICENSE exists.
- The portal has a complete Apache-2.0 LICENSE. The Grafana plugin's package.json
  declares Apache-2.0. Neither settles ownership or licensing of all source files.
- `arima 0.2.0` has no license expression in Cargo metadata but its LICENSE file
  contains Apache License 2.0 text.
- `ring 0.16.20` points to a LICENSE file describing ISC-style and inherited
  BoringSSL material; retain and review its notices and source-file exceptions.
- `webpki 0.22.4` points to a LICENSE with ISC-style terms and additional
  third-party Chromium notices. Do not replace these with a guessed SPDX label.
- `inferno 0.11.21` declares CDDL-1.0. It is pulled in by profiling support;
  review the actual files and distribution obligations before shipping binaries.
- An SPDX string alone does not prove that every required copyright notice or
  bundled native component has been included. The complete license files and
  notice files still need collection and review.

## JavaScript metadata inventory

`admin-ui-dependency-licenses.csv` records 401 locked npm package entries from
`admin-ui/package-lock.json`, including development packages. Seven entries
have no license metadata. Expressions include CC-BY-4.0 and Python-2.0 in
addition to common permissive licenses; retain package-specific notices rather
than assuming the frontend bundle has one uniform dependency license.

The Grafana plugin has no committed npm lockfile, so its complete transitive
license inventory is not established here. Node/npm and frontend builds were
not run in this review. Neither npm metadata nor the Cargo inventory replaces
collecting the license texts, bundled-native notices, fonts and asset provenance.

## Outstanding steps

1. Maintainer confirms authority to release the source, company-owned material,
   copied code, bundled assets and third-party contributions. Git author names
   alone do not establish copyright ownership.
2. Maintainer selects the root project license; then add the full license text,
   Cargo metadata, README wording, and consistent subproject declarations.
3. Complete missing admin UI metadata and resolve/pin Grafana npm dependencies;
   neither inventory covers the provenance of fonts and image assets.
4. Collect required third-party license/notice files for the actual source and
   binary distributions, including embedded native libraries. Record any source
   modifications and exceptions rather than declaring everything Apache-2.0.
5. Review the resulting distribution, not only the top-level dependency list.

The review intentionally leaves the root license unset until the maintainer's
decision is available.

Reference: [GitHub licensing documentation](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/licensing-a-repository).
