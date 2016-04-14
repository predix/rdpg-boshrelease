#!/usr/bin/env bash

set -e
set -x

cf --version
cf api --skip-ssl-validation ${cf_api_url}
cf auth ${cf_admin_username} ${cf_admin_password}

found=false
while read -r line
do
	if [[ ${found} == "true" ]]
	then
		service_name=( $(echo ${line} | awk '{print $1}') )
		case ${service_name} in 
			(service) 
				continue 
				;;
			(*)
        if [[ ${service_name} == "" ]]
        then break
        fi
				echo "Disabling Service Access For ${service_name}"
				cf disable-service-access ${service_name}
				;;
		esac
	else
		case ${line} in
			(*broker:\ ${broker_name}*)
				found=true
				;;
		esac
	fi
done <<< "$(cf service-access)"

echo "Service Access:"
cf service-access

echo "Deleting Service Broker ${broker_name}"
cf delete-service-broker -f ${broker_name}

echo "Service Broker:"
cf service-brokers


