#!/bin/bash
# Script for veryfing and adding connection delay time to specified list
# of IP addresses.

# interface - name of network interface
# vmax - maximum throughput for each IP address
# file - path to file with IP and delay time separated by TAB
interface="enp193s0"
vmax="2048mbit"
file="servers.data"

while true; do
        echo "WARNING: Running this script will erase all queueing disciplines from ${interface}!"
        read -p "Do you want to proceed? (y/n) " yn

        case $yn in
                [yY] ) echo "Script starting...";
                        break;;
                [nN] ) echo "Exiting...";
                        exit;;
                * ) echo invalid response;;
        esac
done

# Delete all queueing disciplines on selected interface
tc qdisc del dev ${interface} root

# CLassfull QDISC setup
tc qdisc add dev ${interface} root handle 1: htb

handle=0

while IFS=$'\t' read -r srv_ip srv_delay; do
        echo "====="
        echo "Server IP is: ${srv_ip} Desired delay time is ${srv_delay}ms"
        echo "Checking ping for ${srv_ip}..."
        srv_rldelay=$(ping -c 6 ${srv_ip} | tail -1 | awk -F '/' '{print $5}')
        echo "Real delay is ${srv_rldelay}ms"
        diff=`echo $srv_delay-$srv_rldelay | bc`
        checkrl=`echo "$srv_rldelay > $srv_delay" | bc`

        if [ "$checkrl" -eq 1 ]; then
                echo "The difference between real ping and desired ping is ${diff}ms, nothing to do here!"
        # Check if the difference between desired delay and real delay is greater than 10ms
        elif [ "$(echo "$diff > 10" | bc)" -eq 1 ]; then
                echo "The difference between real ping and desired ping is ${diff}ms."
                echo "Adding delay to this IP..."
                ((handle=$handle+1))
                tc class add dev ${interface} parent 1: classid 1:${handle} htb rate ${vmax}
                tc qdisc add dev ${interface} parent 1:${handle} netem delay ${diff}ms
                tc filter add dev ${interface} parent 1: protocol ip prio 1 u32 match ip dst ${srv_ip} flowid 1:${handle}
        else
                echo "The difference between real ping and desired ping is ${diff}ms, nothing to do here!"
        fi

done <$file
