#!/bin/sh

setup_git() {
  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "Travis CI"
}

commit_website_files() {
  git add . *.tsv
  echo "# VFB_reporting_results\nrepository containing results of various data change and consistency checking.\n\n Current results are from the latest travis build #$TRAVIS_BUILD_NUMBER from commit: '$TRAVIS_COMMIT_MESSAGE' on $TRAVIS_BRANCH" > README.md
  git add . README.md
  git commit --message "Travis build: $TRAVIS_BUILD_NUMBER "$(date "+%Y%m%d-%H%M%S") 
}

upload_files() {
  git push --quiet origin HEAD:master
}

setup_git
commit_website_files
upload_files
