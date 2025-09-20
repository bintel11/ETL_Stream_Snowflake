



# touch __init__.py files in every folder


New-Item -Path app\__init__.py -ItemType File -Force # type: ignore
New-Item -Path tests\__init__.py -ItemType File -Force # type: ignore
New-Item -Path tests\functional\__init__.py -ItemType File -Force # type: ignore
New-Item -Path tests\integration\__init__.py -ItemType File -Force # type: ignore
New-Item -Path tests\unit\__init__.py -ItemType File -Force # type: ignore
