# Guidance on how to contribute

> All contributions to this project will be released to the public domain.
> By submitting a pull request or filing a bug, issue, or
> feature request, you are agreeing to comply with this waiver of copyright interest.
> Details can be found in our [TERMS](TERMS.md) and [LICENSE](LICENSE).


There are two primary ways to help:
 - Using the issue tracker
 - Changing the code-base


## Using the issue tracker

Use the issue tracker to suggest feature requests, report bugs, and ask questions.
This is also a great way to connect with the developers of the project as well
as others who are interested in this solution.

Use the issue tracker to find ways to contribute. Find a bug or a feature, mention in
the issue that you will take on that effort, then follow the _Changing the code-base_
guidance below.


## Changing the code-base

Generally speaking, you should fork this repository, make changes in your
own fork, and then submit a pull request. All new code should have associated
unit tests (added to `/unit_tests`) that validate implemented features and the presence or lack of defects.
Additionally, the code should follow any stylistic and architectural guidelines
prescribed by the project. In the absence of such guidelines, mimic the styles
and patterns in the existing code-base.

### Guidelines

If you would like to contribute, please follow these steps:

1. Fork the project.
2. Clone your fork: `git clone https://github.com/<github username>/inundation-mapping.git`
3. Create a feature branch: `git checkout -b <dev-your-feature>`
4. Build the Docker container: `Docker build -f Dockerfile -t <image_name>:<tag> <path/to/repository>`
5. Set up pre-commit hooks via pre-commit install. Afterwards when a commit is made it will run `flake8` for code and style linting as well as `flake8-black` to autoformat the code. In many cases issues will be rectified automatically with `flake8-black` but in some cases manual changes will be necessary.
6. Code Standards: Make sure unit tests pass(`pytest unit_tests/`) and there is no significant reduction in code coverage.
Commit your changes: git commit -m 'feature message' This will invoke pre-commit hooks mentioned on step 5 that will lint the code. Make sure all of these checks pass, if not make changes and re-commit.
7. Push to your forked branch: `git push -u origin`, or if the branch is not pushed up yet: `git push --set-upstream origin <your branch>`
8. Open a pull request (review checklist in [PR template](/.github/PULL_REQUEST_TEMPLATE.md) before requesting a review)
