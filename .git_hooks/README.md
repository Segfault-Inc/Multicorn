How to add in a repository
--------------------------

    git submodule add git@github.com:Kozea/.git_hooks.git
    ./.git_hooks/install
    git commit -am "Add hooks"
    git push


When cloning a repo with hooks
------------------------------

    git submodule update --init
    ./.git_hooks/install


To update the hooks
-------------------

    git submodule update

