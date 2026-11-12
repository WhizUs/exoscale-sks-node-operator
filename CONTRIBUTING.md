# Contributing

Thanks for your interest in improving exoscale-sks-node-operator!

## Reporting bugs and requesting features

Open a GitHub issue describing what you expected, what happened, and the
operator version, Kubernetes version and nodepool type (SKS-managed or
Karpenter) involved. For security problems, follow [SECURITY.md](SECURITY.md)
instead of opening an issue.

## Development setup

You need Go (the version in [`go.mod`](go.mod)), `make`, and Docker if you want
to build images or run the e2e tests against [Kind](https://kind.sigs.k8s.io/).
A devcontainer is provided in [`.devcontainer`](.devcontainer).

```sh
make test      # unit tests + envtest
make lint      # golangci-lint
make build     # binary
make test-e2e  # e2e tests in a throwaway Kind cluster
```

The Exoscale API sits behind a small interface with a fake
(`internal/exoscale`), so tests never need real credentials or network access.
Never commit credentials, kubeconfigs or real cluster identifiers.

### Generated files

After changing `api/**/*_types.go` or kubebuilder markers, regenerate and commit
the results:

```sh
make manifests generate
```

Do not hand-edit `config/crd/bases/*`, `config/rbac/role.yaml` or
`zz_generated.*.go`.

## Pull requests

1. Fork the repository and create a branch from `main`.
2. Keep changes focused; add or update tests for behaviour changes.
3. Make sure `make lint test` passes.
4. Use [Conventional Commits](https://www.conventionalcommits.org/) for commit
   messages, e.g. `fix: ...`, `feat: ...`, `refactor: ...`.
5. Open a pull request describing the problem and the change.

New Go files must carry the license header from
[`hack/boilerplate.go.txt`](hack/boilerplate.go.txt).

## License

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE).
