# Production deployment contract

The canonical production release flow is:

1. Create a feature branch from the current `main`.
2. Push the feature branch and open a pull request into `main`.
3. Wait for the GitHub Actions `tests` workflow to complete successfully.
4. Review and merge only after every required check is green.
5. Let Railway deploy the resulting commit from `main`.

Direct pushes to `main` are prohibited by operational policy. A failed or
pending CI run must block merging into `main`.

Repository administrators must configure a GitHub branch protection rule or
ruleset for `main` that requires the `tests` check before merge. Railway must
be configured to deploy only the `main` branch. These controls live in GitHub
and Railway settings and cannot be enforced solely by repository code.

Never bypass a failed check to trigger a production deploy. Fix the failure on
the feature branch, rerun CI, and merge only after the required check succeeds.
