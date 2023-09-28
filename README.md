# resource-pool
[![Build Status](https://github.com/evolution-gaming/resource-pool/workflows/CI/badge.svg)](https://github.com/evolution-gaming/resource-pool/actions?query=workflow%3ACI)
[![Coverage Status](https://coveralls.io/repos/github/evolution-gaming/resource-pool/badge.svg?branch=main)](https://coveralls.io/github/evolution-gaming/resource-pool?branch=main)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/879e88a4e6a94647848bc6b45788a9d7)](https://app.codacy.com/gh/evolution-gaming/resource-pool/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
[![Version](https://img.shields.io/badge/version-click-blue)](https://evolution.jfrog.io/artifactory/api/search/latestVersion?g=com.evolutiongaming&a=resource-pool_2.13&repos=public)

Pool of [cats-effect](https://typelevel.org/cats-effect/) resources

## Features:
* allocates resources on demand up to configured limit
* deallocates resources if not active for a configured time
* tries to minimize number of resources in the pool
* uses first-in-first-out queue for tasks
* shuts down gracefully after completing accumulated tasks
* tolerates resource failures
* supports cancellation of `resouce.use` 

## Example 

```scala
import com.evolution.resourcepool.ResourcePool.implicits._

trait Connection {
  def query(): IO[Any]
}

def connection: Resource[IO, Connection] = ???

connection
  .toResourcePool( // you can convert any resource into the pool of resources
    maxSize = 10, // it will create up to `maxSize` connections 
    expireAfter = 1.minute) // pool will release connection if it is not used for 1 minute
  .use { connectionPool =>
    connectionPool
      .resource // this will get first available connection or allocate one
      .use { connection =>
        connection.query() // here you have access to the connection
      }
  }

```

## Setup

```scala
addSbtPlugin("com.evolution" % "sbt-artifactory-plugin" % "0.0.2")

libraryDependencies += "com.evolution" %% "resource-pool" % "0.0.3"
```