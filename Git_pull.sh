echo "Checking Git is installed....." /n ;
git --version  || { echo "Git is not installed..." ; exit 1; }

# Prompt user for input
echo "Enter the path to pull:"
read user_input

cd $user_input

echo "Configuring your Git username and email..";

git config --global user.name "anu41m"
git config --global user.email "anoopmadhu2000@gmail.com"

echo "Initializing the folder as a Git repository....."
git init


echo " Adding all files in the folder to the Git staging area..."
git add .

# echo "Time to select files:" & sleep 30

echo "Enter Commit message: "
read user_input

echo "Committing the changes ...."

git commit -m \"$user_input\"

echo "Enter the Branch to pull the changes:"
echo "Available Brances:- \"DEV_test\""
read $user_input

case "$user_input" in
"DEV_test")
git remote add origin https://github.com/anu41m/my_jupyter_project
git branch -M DEV_test
git push -u origin DEV_test
;;
*)
echo "Invalid input!"
;;
esac
# echo "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"