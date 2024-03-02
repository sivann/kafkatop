tag=$1
git tag -d $tag
git push origin :${tag}
