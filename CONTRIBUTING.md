# Guidance on how to contribute

> All contributions to this project will be released to the public domain.
> By submitting a pull request or filing a bug, issue, or
> feature request, you are agreeing to comply with this waiver of copyright interest.
> Details can be found in our [LICENSE](LICENSE).


There are two primary ways to help:
 - Using the issue tracker (Bug Reporting, Submitting Feedback)
 - Changing the code-base (Bug fixes, Feature Implementation & Improving Documentation)

If you have problems at any time, please refer to our [README.md](./README.md) file for contact information
in the "Getting Involved" section.


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
    Docker build -f Dockerfile.dev -t <image_name>:<tag> <path/to/repository>
    ```


### Linting
   In order to push a branch up to the origin repo, you will need to do some linting checks. As branches are pushed against
   a PR, it will automatically run linting tools. While it is acceptable to not run linting on each push the commit to the branch,
   you will need to fix linting issues before submitting the final commit for merging to the origin branch repo.
   
   If you do not run linting steps, and an linting error is found, you should get an email with a link to drill down to
   find the details, which you can go back to your code to fix the linting changes.

   Automatic linting tests are done when submitting branches with associated PRs.

1. Pre-commit host machine installation:

   [pre-commit](https://pre-commit.com/) is used to run auto-formatting and enforce styling rules.
   It is a critical part of development and may be enforced at the 'git commit' step. If you intend to execute `flake8`, `black`
   or `isort` from the command, additional configuration and installation is required.

   Linting / pre-commit can not be done inside the docker container and must be done on your host machine.

   **Note: These steps below are similar to another required critical step (pre-commit configuration) later in this document, which also needs to be run**.

   **Important: For all commands, you may have to add the word "sudo" in front of your commands, often related to permissions issues.**

   If pre-commit is not already installed on your system:
   ```
   pip install pre-commit
   ```
   If it is already installed, consider upgrading it to at least v4.5.1.
   ```
   pip install --upgrade pre-commit
   ```
   All related tools (git hook scripts) are installed under the `pre-commit install` step, not at this level. See https://pre-commit.com/#install
   
   If you get an error message during the installation of pre-commit which says:
   
   ```
   *Installing collected packages: pre-commit
       WARNING: The script pre-commit is installed in '/home/{your_user_name}/.local/bin' which is not on PATH.
       Consider adding this directory to PATH or, if you prefer to suppress this warning, use --no-warn-script-location.`,
   then you will need to do some additional configuration. You need to adjust your path.*
   
   (Adjusting the path to be exactly the path you see in the WARNING message above from your console output).
       export PATH="/home/{your_user_name}/.local/bin:$PATH"
   ```
   To test that it installed correctly and is pathed correctly and check the version:
   ```
   pre-commit --version
   ```
   It should respond with the phrase *pre-commit 4.5.1* (or higher).


2. pre-commit local branch configuration:
   
   Now, you need to configure your local clone of the repo to honor the pre-commit hooks.
   The `pre-commit` package is used to pick up the pre-commit hooks which verify your staged changes adhere to the project's style and format requirements (configuration defined in [pyproject.toml](/pyproject.toml)).

   Initialize the pre-commit hooks included within the root directory of this code repository (`inundation-mapping`):
    ```
    $ pre-commit install
    ```
    
3. With `pre-commit` installed on your host machine and when the commit command is used, it will run the pre-commit hooks defined in
   [pre-commit-config.yaml`](.pre-commit-config.yaml) in the local branch folder.
   For reference, you may run any of the pre-commit hooks manually before issuing the `git commit` command (see below). 

    It is recommended that you run the linting commands before running 'git add' or 'git commit'.

    There are multiple ways to run pre-commit tests. Here are three:
    ```
    # Run only the isort, black, flake8 (in order).
    pre-commit run -a isort  (*** See the Tech-Tip on re-running isort and black a second time)
    pre-commit run -a black  (*** See the Tech-Tip on re-running isort and black a second time)
    pre-commit run -a flake8  (you will have to fix errors returned here to push your repo changes)

    # Or, check only the staged changes
    pre-commit run

    # Or, check all files in the repo
    pre-commit run -a
    ```
    ### Note:
    ** Reminder: some environments may require add the word **sudo** in front commands often due to permissions issues.

    ### Tech-Tip:
    If you run isort, black and flake8 one at a time as above in order, you can take advantage of a linting feature.
    When you run either isort and/or black, it auto updates the files and fixes the file, assuming it has permissions (sudo).
    Then run it again to validate isort or black another time to see if it now passes. "flake8" does not auto update files
    and you will need fix or address errors by hand.
    
    If you have already done the 'git add or 'git commit' and linting changes some files, you will need to re-add
    and re-commit. We recommend you do the pre-commit tests before even the running git add or git commit.
    <br/>

### Committing your changes:

Run the typical 'git add' and 'git commit' commands.
    ```
    'git add -a'
    'git commit -m {<descriptive sentence or two of changes>}'
    ```
Git push will automatically invoke pre-commit hooks mentioned in the linting section above 
but only if the commit is being submitted to a branch attached to a PR.
   
1.  Push to your forked or non-forked branch.
    If this is the first time, pushing your local branch to GitHub, you will need to tell the origin branch your new
    branch name.
    ```
    git push --set-upstream origin <your branch>
    ```
    For subsequent pushs, you will simple run "git push".


2. Submit a pull request on [inundation-mapping's GitHub page](https://github.com/NOAA-OWP/inundation-mapping).
   Please review checklist in [PR template](/.github/PULL_REQUEST_TEMPLATE.md) for additional PR guidance.
   Please contact us when you are ready for PR to be merged into the origin parent branches.
   Please refer to our [README.md](./README.md) file for contact information in the "Getting Involved" section.
   