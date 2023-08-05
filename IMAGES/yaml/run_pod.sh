#!/bin/bash

## pod 1
k run nginx --image=nginx:latest

# pod 2
k run nginx --image=nginx:latest --overrides='{ "spec": { "terminationGracePeriodSeconds":0 } }'
