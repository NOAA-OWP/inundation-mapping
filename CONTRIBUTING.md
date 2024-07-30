# Guidance on how to contribute

> All contributions to this project will be released to the public domain.
> By submitting a pull request or filing a bug, issue, or
> feature request, you are agreeing to comply with this waiver of copyright interest.
> Details can be found in our [LICENSE](LICENSE).


There are two primary ways to help:
 - Using the issue tracker (Bug Reporting, Submitting Feedback)
 - Changing the code-base (Bug fixes, Feature Implementation & Improving Documentation)


## Using the issue tracker

Use the issue tracker to suggest feature requests, report bugs, and ask questions.
This is also a great way to connect with the developers of the project as well
as others who are interested in this solution.

Use the issue tracker to find ways to contribute. Find a bug or a feature, mention in
the issue that you will take on that effort, then follow the _Changing the code-base_
guidance below.


## Changing the code-base

Generally speaking, you should fork this repository, make changes in your
own fork, and then submit a pull request. 
Additionally, the code should follow any stylistic and architectural guidelines
prescribed by the project. In the absence of such guidelines, mimic the styles
and patterns in the existing code-base.

### Guidelines

If you would like to contribute, please follow these steps:

1. Fork the project on the GitHub webpage.
2. Clone your fork:
    ```
    $ git clone https://github.com/<github username>/inundation-mapping.git
    $ cd inundation-mapping
    ```
3. Create a local branch:
    ```
    git checkout -b <dev-your-bugfix-or-feature>
    ```

4. Build the Docker container (if not already done in the [README.md](./README.md) page.):
    ```
    Docker build -f Dockerfile -t <image_name>:<tag> <path/to/repository>
    ```

5. Pre-commit installation:

   [pre-commit](https://pre-commit.com/) is used to run auto-formatting and enforce styling rules.
   It is a critical part of development and is enforced at the 'git commit' step. Key tools are included **inside the docker container** if you want to execute the correctly configured linting and formatting command line executables there. If you intend to execute `flake8`, `black` or `isort` from the command line **outside of the docker container**, additional configuration and installation is required, which will not be described here.
   
   **Note: These steps below are similar to another required critical step (pre-commit configuration) later in this document, which also needs to be run**.

   If pre-commit is not already installed on your system:
   ```
   pip install pre-commit
   ```
   All related tools (git hook scripts) are installed under the `pre-commit install` step, not at this level. See https://pre-commit.com/#install
   
   If you get an error message during the install of pre-commit which says:
   
   *Installing collected packages: pre-commit
       WARNING: The script pre-commit is installed in '/home/{your_user_name}/.local/bin' which is not on PATH.
       Consider adding this directory to PATH or, if you prefer to suppress this warning, use --no-warn-script-location.`,
   then you will need to do some additional configuration. You need to adjust your path.*
   ```
   (Adjusting the path to be exactly the path you see in the WARNING message above from your console output).
   export PATH="/home/{your_user_name}/.local/bin:$PATH"
   ```
   To test that it installed correctly, is pathed correctly and check the version:
   ```
   pre-commit --version
   ```
   It should respond with the phrase *pre-commit 3.6.0* (version may not be exact).


6. pre-commit configuration:
   
   Now, you need to configure your local clone of the repo to honor the pre-commit hooks.
   The `pre-commit` package is used to pick up the pre-commit hooks which verify your staged changes adhere to the project's style and format requirements (configuration defined in [pyproject.toml](/pyproject.toml)).

   Initialize the pre-commit hooks included within the root directory of this code repository (`inundation-mapping`):
    ```
    $ pre-commit install
    ```
    
7. At this point, you should be set up with `pre-commit`. When a commit is made it will run the pre-commit hooks defined in [`.pre-commit-config.yaml`](.pre-commit-config.yaml). For reference, you may run any of the pre-commit hooks manually before issuing the `git commit` command (see below). Some tools used by the pre commit git hook (`isort`, `flake8`, & `black`) are also available as command line executables **within the Docker container***, however, it is recommended to run them through `pre-commit` **outside of the container**, as it picks up the correct configuration.

   ```
   There are multiple ways to run pre-commit tests. Here are three:
   
   # Check only the staged changes
   pre-commit run

   # Check all files in the repo
   pre-commit run -a

   # Run only the isort, black, flake8 (in order).
   pre-commit run -a isort
   pre-commit run -a black  (If it fails the first time, run it again and now apply a fix if required)
   pre-commit run -a flake8
   ```
    **Note: The pre-commit system can be a bit tricky. If you have trouble with pre-commit steps above, you have another option.** Some environments may require add the word **sudo** in front commands .
   ```
   # Open up a docker container
   cd /foss_fim
   isort .
   black .
   flake8 .

   # optionally close the container
   # Back on your terminal console (outside the container), use the typical git add, git commit, git push


8. Outside of the Docker container, commit your changes:
    ```
    git commit -m "<descriptive sentence or two of changes>"
    ```
    This will invoke pre-commit hooks mentioned in step 6 that will lint & format the code (some others as well). In many cases non-compliant code will be rectified automatically, but in some cases manual changes will be necessary. Make sure all of these checks pass. If not, make necessary changes (`git add <...>`), and re-issue `git commit -m "<...>"`.
   
9.  Push to your forked branch:
    ```
    git push -u origin
    ```
    or if the branch is not pushed up yet:
    ```
    git push --set-upstream origin <your branch>
    ```

11. Submit a pull request on [inundation-mapping's GitHub page](https://github.com/NOAA-OWP/inundation-mapping) (please review checklist in [PR template](/.github/PULL_REQUEST_TEMPLATE.md) for additional PR guidance).
   
