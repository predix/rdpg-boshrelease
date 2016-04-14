# RDPG Agent w/ Cloud Foundry

`rdpg` is configured to listen on port 8888 by default (configurable) with
an http API listener that allows for [Cloud Foundry Service Broker]() functionality.

In development `rdpg` can be registered with CF via,
```sh
CF_TRACE=true cf create-service-broker rdpg cfadmin cfadmin http://10.244.2.2:8888
```

In production you will need to make sure that a domain name passes through to 
this backend port on any of the hosts, first host by default.

Don't forget to allow access to the newly registered services,
```sh
cf enable-service-access rdpg -o $USER
cf service-access
cf marketplace
```


