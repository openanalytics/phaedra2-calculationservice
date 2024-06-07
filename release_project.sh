#!/bin/bash

# Stop script execution if any command fails
set -e

parent_id=`mvn -q -Dexec.executable=echo -Dexec.args='${project.parent.artifactId}' --non-recursive exec:exec`

# --------------------------------------------------------------
# Step 1: determine current and release version numbers
# --------------------------------------------------------------

current_version=`mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec`
release_version=${current_version/-SNAPSHOT/}
echo "The current version is: $current_version"
echo "The release version is: $release_version"

# --------------------------------------------------------------
# Step 2: modify version numbers in the POM files
# --------------------------------------------------------------

mvn versions:set -DnewVersion="$release_version"
mvn versions:update-properties -Dincludes=eu.openanalytics.phaedra -U
mvn versions:update-child-modules

if [ "$parent_id" = "phaedra2-parent"]; then
  mvn versions:update-parent -U
fi

# Commit updated pom files
git add pom.xml */*pom.xml
git commit -m "Updated version to $release_version"

# --------------------------------------------------------------
# Step 3: gitflow release
# --------------------------------------------------------------

mvn -B -DskipTestProject=true -DpushRemote=false gitflow:release-start gitflow:release-finish

# --------------------------------------------------------------
# Step 4: modify version numbers in the POM files (for the next snapshot)
# --------------------------------------------------------------

# Note: the main pom version has already been modified by gitflow
mvn versions:update-child-modules -DallowSnapshots=true
mvn versions:update-properties -DallowSnapshots=true -Dincludes=eu.openanalytics.phaedra -U

if [ "$parent_id" = "phaedra2-parent"]; then
  mvn versions:update-parent -DallowSnapshots=true -U
fi

# Commit updated pom files
git add pom.xml */*pom.xml
git commit -m "Updated version to the next development snapshot"

# --------------------------------------------------------------
# Step 5: push all branches and tags, cleanup
# --------------------------------------------------------------

#git push origin develop master --tags
find . -name "pom.xml.versionsBackup" -type f | xargs rm