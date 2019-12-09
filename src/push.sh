#!/bin/sh

setup_git() {
  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "Travis CI"
}

commit_website_files() {
  git add . *.tsv
  echo "# VFB_reporting_results\nRepo for the results of pipelines reporting dataflow to and within VFB.\n\n Current results are from the latest travis build #$TRAVIS_BUILD_NUMBER from commit: '$TRAVIS_COMMIT_MESSAGE' on $TRAVIS_BRANCH\n\n" > README.md
  cat ../reports.md >> README.md
  git add . README.md
  git commit --message "Travis build: $TRAVIS_BRANCH-$TRAVIS_BUILD_NUMBER "$(date "+%Y%m%d-%H%M%S") 
}

upload_files() {
  git push --quiet origin HEAD:master
}

setup_git
commit_website_files
upload_files
