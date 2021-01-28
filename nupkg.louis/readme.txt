1、由于*.unpkg文件存在于.gitignore列表中，把文件*.nupkg_修改文件后缀为.unpkg
2、打开nuget程序包管理器，增加一个程序包源Package source，路径指向nupkg文件所在文件夹
3、nuget程序包源指向Package source，生成解决方案会将引用的文件复制，只要生成一遍即可，程序包源再重新设定