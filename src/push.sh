#!/bin/sh

setup_git() {
  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "Travis CI"
}

commit_website_files() {
  git add . *.tsv
  echo "Results from latest Travis build: $TRAVIS_BUILD_NUMBER" > readme.md
  git add . readme.md
  git commit --message "Travis build: $TRAVIS_BUILD_NUMBER"
}

upload_files() {
  git push --quiet origin HEAD:master
}

setup_git
commit_website_files
upload_files
