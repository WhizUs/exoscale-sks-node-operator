# Security Policy

## Supported versions

Only the latest release receives security fixes. Please upgrade to the newest
`v0.x` tag before reporting an issue against an older one.

## Reporting a vulnerability

**Please do not open a public issue, pull request or discussion for a security
problem.**

Report it privately through GitHub's
[private vulnerability reporting](https://github.com/WhizUs/exoscale-sks-node-operator/security/advisories/new)
(the **Security** tab → **Report a vulnerability**).

Helpful details include:

- the affected version or commit,
- a description of the issue and its impact,
- steps to reproduce, or a proof of concept,
- any suggested fix or mitigation.

We will acknowledge your report, keep you informed while we investigate, and
credit you in the advisory unless you prefer to remain anonymous.

## Scope

This operator holds Exoscale API credentials and can cordon, drain, scale and
evict cluster nodes. Issues of particular interest include:

- leaking or logging the Exoscale API key or secret,
- RBAC permissions broader than the operator needs,
- ways to make the operator disrupt nodes outside its policy, maintenance
  window or concurrency limits.
