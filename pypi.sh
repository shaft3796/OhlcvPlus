# Publish the package to pypi
# Usage: ./pypi.sh
# Prepare the package
python setup.py sdist bdist_wheel
# Upload the package
twine upload dist/*