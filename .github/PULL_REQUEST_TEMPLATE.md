[Short description explaining the high-level reason for the pull request]

### Additions

### Changes

### Removals

---------------------------------------------------------------
### Testing
Generally, you do not copy this part into the ChangeLog, but you can.

These are some quick notes on what you did test and/or notes for the reviewer to help with their review testing.

****************************************************************
### From here down, do not include in the changelog.md. 

### Notes to DevOps Team or others:
Please add any notes that are helpful for us to make sure it is all done correctly. Do not put actual server names or full true paths, just shortcut paths like 'efs..../inputs/,  or 'dev1....inputs', etc. Also see the Deployment Plan section below. More details the better. 

---------------------------------------------------------------
### Developer checklist (For developer use)

You may update this checklist before and/or after creating the PR. If you're unsure about any of them, please ask, we're here to help! These items are what we are going to look for before merging your code.

- [ ] If there are any new or updated inputs, docker file changes, pipfile, or python changes, did you fill out the deployment plan section below?
- [ ] Add yourself as an [assignee](https://docs.github.com/en/issues/tracking-your-work-with-issues/assigning-issues-and-pull-requests-to-other-github-users 
- [ ] Informative and human-readable title, using the format: `[_pt] PR: <description>`
- [ ] Links are provided if this PR resolves an issue, or depends on another other PR
- [ ] If submitting a PR to the `dev` branch (the default branch), you have a descriptive [Feature Branch](https://www.atlassian.com/git/tutorials/comparing-workflows/feature-branch-workflow) name using the format: `dev-<description-of-change>` (e.g. `dev-revise-levee-masking`)
- [ ] The feature branch you're submitting as a PR is up to date (merged) with the latest `dev` branch
- [ ] Have the linting tools be run?
- [ ] Any _change_ in functionality is tested
- [ ] New functions are documented (with a description, list of inputs, and expected output)
- [ ] Placeholder code is flagged / future todos are captured in comments. Please use the convention of "TODO (date)" (case-sensitive) and description of the future fix.
- [ ] [CHANGELOG](/docs/CHANGELOG.md) updated with template version number, e.g. `4.x.x.x`
- [ ] Where applicable, has fim_pipeline been tested with multiple HUCs, including some other unaffected HUCs?
- [ ] If applicable, have you worked with Devops for new or updated docker images, pip files, inputs, pre-clips, etc. **CRITICAL: If yes, please update the Deployment Plan section and work with DevOps on it.**


---------------------------------------------------------------
### Reviewer / Approver Checklist
- [ ] Where applicable, has fim_pipeline been tested with muliple HUCs, including some other unaffected HUCs?
- [ ] Does it have latest dev branch merged in?
- [ ] Is the changelog complete?
- [ ] If there are new inputs, have you confirmed that they have been copied to all enviroments?

---------------------------------------------------------------
### Deployment Plan (For FIM developers use)
- **Does the change impact inputs, docker or python packages?**
    - [ ] Yes
    - [ ] No  (f no.. skip the rest of the Deployment Plan section)
    
-  **If you are not a FIM dev team member:  Please let us know what you need and we can help with it.**

-  **If you are a FIM Dev team member:** 
    -  Please work with the DevOps team and do not just go ahead and do it without some co-ordination.
    -  Copy where you can, assign where you can not, and it is your responsibility to ensure it is done. Please ensure it is completed before the PR is merged. 

    - Require new or adjusted data inputs? Does it have a way to version (folder or file dates)?
       - [ ] Yes
       - [ ] No
       - Remember to run downstream input tools such as make_dem_diff_for_bridges or pre-clip, etc (depending on what it applicable. Update Bash Variables and run fim-pieline run on at least one HUC.)
       - Has the inputs been copied in all four enviros:  (Note: FIM S3 is normally the full back up within reason). ESIP does not always get new input data, but most of the time it does. And they often get only the final files required to fim-pipeline or other tools to run, not the interium files.
            - [ ] FIM EFS  
            - [ ] FIM S3
            - [ ] ESIP

    - Has new or updated python packages, PipFile, Pipefile.lock or Dockerfile changes?  DevOps can help or take care of it if you want. Just need to know if it is required.
       - [ ] Yes
       - [ ] No

                  
- Please use caution in removing older version unless it is at least two versions ago.  Confirm with DevOps if cleanup might be involved.
- Please update the [FIM Inputs guide](https://docs.google.com/document/d/1JyPc6wPRvQG3QTvcZ2UAxHYbCHwlwpI_R_Vgm7-gfIY/edit?pli=1&tab=t.0#heading=h.poz5e9q5wgqf) or ask DevOps to do it.

---------------------------------------------------------------
### Merge Checklist (For Technical Lead use only)

- [ ] Update [CHANGELOG](/docs/CHANGELOG.md) with latest version number and merge date
- [ ] Update the [Citation.cff](/CITATION.cff) file to reflect the latest version number in the [CHANGELOG](/docs/CHANGELOG.md)
- [ ] If applicable, update [README](/README.md) with major alterations
