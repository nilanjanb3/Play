#!/bin/bash

set -euo pipefail

HEADER="\033[1;36m"
NC="\033[0m"

# Print the header line
printf "${HEADER}%-35s %-70s${NC}\n" "Role Name" "Relevant Policies"
printf "%-35s %-70s\n" "-----------------------------------" "----------------------------------------------------------------------"

# Get the list of roles
roles=$(aws iam list-roles --output json | jq -r '.Roles[].RoleName')

# Loop through each role
for role in $roles; do
    policies=()

    # Get the list of attached managed policies
    attached_policies=$(aws iam list-attached-role-policies --role-name "$role" --output json | jq -r '.AttachedPolicies[]?.PolicyName')
    # Loop through each policy
    for policy in $attached_policies; do
        # Check if the policy name contains "admin", "fullaccess", "Administrator", "FullAccess", or "Admin"
        if [[ "$policy" =~ admin|fullaccess|Administrator|FullAccess|Admin ]]; then
            policies+=("$policy")
        fi
    done

    # Get the list of inline policies
    inline_policies=$(aws iam list-role-policies --role-name "$role" --output json | jq -r '.PolicyNames[]?')
    # Loop through each policy
    for policy in $inline_policies; do
        # Check if the policy name contains "admin", "fullaccess", "Administrator", "FullAccess", or "Admin"
        if [[ "$policy" =~ admin|fullaccess|Administrator|FullAccess|Admin ]]; then
            policies+=("$policy")
        fi
    done

    # If the role has any relevant policies, print the role name and relevant policies
    if [ ${#policies[@]} -gt 0 ]; then
        printf "%-35s %-70s\n" "$role" "$(IFS=', '; echo "${policies[*]}")"
    fi
done

