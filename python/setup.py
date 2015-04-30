from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize

ext_modules=[
  Extension(
    "jlog",
    sources = ["jlog.pyx"],
    libraries = ["jlog"],
    include_dirs = [
      "/usr/local/include",
      "/usr/include"],
    library_dirs = [
      "/usr/local/lib",
      "/usr/lib"]
  )
]

setup(
  name = "jlog",
  version = "1.0",
  description = "JLog Python Library",
  url = "https://labs.omniti.com/labs/jlog",
  ext_modules = cythonize(ext_modules)
)
