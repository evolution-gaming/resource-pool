# resource-pool
[![Build Status](https://github.com/evolution-gaming/resource-pool/workflows/CI/badge.svg)](https://github.com/evolution-gaming/resource-pool/actions?query=workflow%3ACI)
[![Coverage Status](https://coveralls.io/repos/github/evolution-gaming/resource-pool/badge.svg?branch=master)](https://coveralls.io/github/evolution-gaming/resource-pool?branch=master)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/fab03059b5f94fa5b1e7ad7bddfe8b07)](https://www.codacy.com/app/evolution-gaming/resource-pool?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=evolution-gaming/resource-pool&amp;utm_campaign=Badge_Grade)
[![Version](https://img.shields.io/badge/version-click-blue)](https://evolution.jfrog.io/artifactory/api/search/latestVersion?g=com.evolutiongaming&a=resource-pool_2.13&repos=public)

Pool of cats-effect resources

## Features:
* allocates resources on demand up to configured limit
* deallocates resources if not active for a configured time
* tries to minimize number of resources in the pool
* uses first-in-first-out queue for tasks
* shuts down gracefully after completing accumulated tasks
* tolerates resource failures

## Setup

```scala
addSbtPlugin("com.evolution" % "sbt-artifactory-plugin" % "0.0.2")

libraryDependencies += "com.evolution" %% "resource-pool" % "0.0.1"
```