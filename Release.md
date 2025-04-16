# Release

Create the release and uploaded it to pypi testing:

```shell
vim setup.py
python3 setup.py sdist bdist_wheel
tar tzf dist/mycrypto-*.tar.gz
twine check dist/*
twine upload --repository-url https://test.pypi.org/legacy/ dist/*
```

Test the release on pypi testing.
Upload the release to pypi:

```shell
twine upload dist/*
```

Tag the release with `git tag` and push the tag to the git server.
Create hashes:

```shell
.venv/bin/pip hash dist/mycrypto-*-py3-none-any.whl >> version_hashes.txt
.venv/bin/pip hash dist/mycrypto-*.tar.gz >> version_hashes.txt
```

Now format the hashes and create a pull request.
