#!/bin/sh

setup_git() {
  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "Travis CI"
}

commit_website_files() {
  git add . results/*.tsv
  echo "Results from latest Travis build: $TRAVIS_BUILD_NUMBER" > reports/readme.md
  git add . results/readme.md
  git commit --message "Travis build: $TRAVIS_BUILD_NUMBER"
}

upload_files() {
  git push --quiet origin HEAD:$TRAVIS_BRANCH
}

setup_git
commit_website_files
upload_files
