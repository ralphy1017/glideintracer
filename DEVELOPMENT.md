<!--
SPDX-FileCopyrightText: 2017 Fermi Research Alliance, LLC
SPDX-License-Identifier: Apache-2.0
-->

# Gidein Tracer

Glidein TRacer development

# Developer Workflow

This project uses the [GitHub flow](https://docs.github.com/en/get-started/quickstart/github-flow)

The main repo is hosted on GitHub: https://github.com/glideinWMS/glideintracer

We recommend to use the [GitHub CLI](https://cli.github.com/) to easily authenticate and interact with GitHub.
[Here]() are the install istructions, e.g. on RedHat based distros (dnf):
```
sudo dnf install 'dnf-command(config-manager)'
sudo dnf config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo
sudo dnf install gh
```
Then you can use commands like `gh auth login`.

# Getting Started with development

NOTE: This project has a pre-commit config.
To install it run `pre-commit install` from the repository root.
You may want to setup automatic notifications for pre-commit enabled
repos: https://pre-commit.com/index.html#automatically-enabling-pre-commit-on-repositories

Make sure you have up to date versions of `pip`, `setuptools`, and `wheel`.

`python3 -m pip install --upgrade pip setuptools wheel --user`

Now you can perform an editable install of the code base.

`python3 setup.py develop --user`

From here `pytest` should "just work".

NOTE:

- For running tests, make sure you have `C.UTF-8` locale defined

```shell
sudo localedef -v -c -i en_US -f UTF-8 C.UTF-8
```

## Licensing compliance

Planets Math is released under the Apache 2.0 license and license compliance is
handled with the [REUSE](http://reuse.software/) tool.
REUSE is installed as development dependency or you can install it manually
(`pip install reuse`). All files should have a license notice:

- to check compliance you can use `reuse lint`. This is the command run also by the pre-commit and CI checks
- you can add on top of new files [SPDX license notices](https://spdx.org/licenses/) like
  ```
  # SPDX-FileCopyrightText: 2022 Fermi Research Alliance, LLC
  # SPDX-License-Identifier: Apache-2.0
  ```
- or let REUSE do that for you (`FILEPATH` is your new file):
  ```
  reuse addheader --year 2022 --copyright="Fermi Research Alliance, LLC" \
    --license="Apache-2.0" --template=compact FILEPATH
  ```
- Files that are not supported and have no comments to add the SPDX notice
  can be added to the `.reuse/dep5` file
- New licenses can be added to the project using `reuse download LCENSEID`. Please
  contact project management if this is needed.
