# Wordpress participants

Wordpress hosts plugins in a subversion respository.
These plugins usually contain readme files with the email adresses of the authors.
As downloading the full repository is time consuming, the following commands let you download the relevant files only.

Checkout the first level directory stucture (one directory per plugin):

```
svn co --depth files https://plugins.svn.wordpress.org/
```

Iterate over the directories and checkout their immediate content (trunk, tags, etc.):

```
for file in *; do svn up --set-depth immediates $file; done
```

Iterate over the directories again and checkout the files stored in the trunk directories:

```
for file in *; do svn up --set-depth immediates $file/trunk; done
````

Search for the emails in the directory structure:

```
python3 extract-emails.py wordpress_dir emails.txt
```

