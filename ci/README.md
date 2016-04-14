Setup pipeline in Concourse
---------------------------

```
fly -t snw c -c pipeline.yml --vars-from credentials.yml rdpg-boshrelease
```

Building/updating the base Docker image for tasks
-------------------------------------------------

Each task within all job build plans uses the same base Docker image for all dependencies. Using the same Docker image is a convenience. This section explains how to re-build and push it to Docker Hub.

All the resources used in the pipeline are shipped as independent Docker images and are outside the scope of this section.

```
fly -t snw configure \
  -c ci_image/pipeline.yml \
  --vars-from credentials.yml \
  rdpg-boshrelease-image
```

This will ask your targeted Concourse to pull down this project repository, and build the `ci_image/Dockerfile`, and push it to a Docker image on Docker Hub.
