#!/bin/bash

echo -e "Checking Git is installed.....\n"
git --version || { echo "Git is not installed..."; exit 1; }

# Navigate to the desired directory
cd "/Users/anoopm/my_jupyter_project" || { echo "Directory not found!"; exit 1; }

echo "Configuring your Git username and email..."
git config --global user.name "anu41m"
git config --global user.email "anoopmadhu2000@gmail.com"

echo "Initializing the folder as a Git repository..."
git init

echo "Adding all files in the folder to the Git staging area..."
git add .

echo "Enter Commit message: "
read commit_message

echo "Committing the changes..."
git commit -m "$commit_message"

echo "Available Branches: dev"
echo "Enter the Branch to pull the changes:"
read branch_name

case "$branch_name" in
"dev")
    git push origin dev
    ;;
*)
    echo "Invalid input! Aborting."
    ;;
esac
