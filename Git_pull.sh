# Prompt user for input
echo "Enter the path to pull:"
read user_input

cd $user_input

git config --global user.name "anu41m"
git config --global user.email "anoopmadhu2000@gmail.com"

git init

git add .

echo "Time to select files:" & sleep 30

echo "Enter Commit message: "
read user_input

git commit -m $user_input

git remote add origin https://github.com/anu41m/my_jupyter_project.git

git branch -M DEV_test
git push -u origin DEV_test