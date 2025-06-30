[Short description explaining the high-level reason for the pull request]

### Additions

### Changes

### Removals

---------------------------------------------------------------
### Testing
Generally, you do not copy this part into the ChangeLog. These are some quick notes on what you did test or what the reviewer to know for their tests.


---------------------------------------------------------------
### Deployment Plan (For developer use)
- **Does the change impact inputs, docker or python packages?**
    - [ ] Yes
    - [ ] No
- Has new or updated python packages or Dockerfile changes?
    - [ ] Yes
- Require new or adjusted data inputs? Does it have a way to version (folder or file dates?)
    - [ ] Yes
- If new or updated data sets, has the FIM code been updated and tested with the new/adjusted data (a test against subset is fine)?
    - [ ] Yes
- Require new pre-clip set?
    - [ ] Yes
- If applicable, has a deployment plan be created with the DevOps team? including what was run already, what needs to be copied to all 4/5 enviros, from where, what still needs to be run at scale, etc. Any notes are helpful.
    - [ ] Yes
    - [ ] I will shortly. :)

**Notes to DevOps Team:**

---------------------------------------------------------------
### Issuer Checklist (For developer use)

You may update this checklist before and/or after creating the PR. If you're unsure about any of them, please ask, we're here to help! These items are what we are going to look for before merging your code.

- [ ] Informative and human-readable title, using the format: `[_pt] PR: <description>`
- [ ] Links are provided if this PR resolves an issue, or depends on another other PR
- [ ] If submitting a PR to the `dev` branch (the default branch), you have a descriptive [Feature Branch](https://www.atlassian.com/git/tutorials/comparing-workflows/feature-branch-workflow) name using the format: `dev-<description-of-change>` (e.g. `dev-revise-levee-masking`)
- [ ] Changes are limited to a single goal (no scope creep)
- [ ] The feature branch you're submitting as a PR is up to date (merged) with the latest `dev` branch
- [ ] `pre-commit` hooks were run locally
- [ ] Any _change_ in functionality is tested
- [ ] New functions are documented (with a description, list of inputs, and expected output)
- [ ] Placeholder code is flagged / future todos are captured in comments
- [ ] [CHANGELOG](/docs/CHANGELOG.md) updated with template version number, e.g. `4.x.x.x`
- [ ] Add yourself as an [assignee](https://docs.github.com/en/issues/tracking-your-work-with-issues/assigning-issues-and-pull-requests-to-other-github-users) in the PR  as well as the FIM Technical Lead

---------------------------------------------------------------
### Merge Checklist (For Technical Lead use only)

- [ ] Update [CHANGELOG](/docs/CHANGELOG.md) with latest version number and merge date
- [ ] Update the [Citation.cff](/CITATION.cff) file to reflect the latest version number in the [CHANGELOG](/docs/CHANGELOG.md)
- [ ] If applicable, update [README](/README.md) with major alterations
