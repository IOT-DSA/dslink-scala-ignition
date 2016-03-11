#!/usr/bin/env bash
sbt clean compile dist
cp target/universal/dslink-scala-ignition-0.1.0-SNAPSHOT.zip ../../files/dslink-scala-ignition.zip
