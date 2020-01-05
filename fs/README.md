
    glob: (globpath) => glob.sync(globpath),
    // 写文件
    write: (file, data, mode, format) => write(file, data, mode, format),
    // 读文件
    read: (file, format, code) => read(file, format, code),
    // 读单行
    readline: (file, lineFn, doneFn) => readline(file, lineFn, doneFn),
    // 检查文件是否存在
    exist: (file) => fs.existsSync(file),

    // 返回文件最后更新时间
    info: (file) => (fs.existsSync(file)) ? fs.statSync(file) : 0,

    // 创建目录
    mkdir: (file) => md.sync(path.dirname(file)),
    // 返回文件名
    basename: (file) => path.basename(file)"# fs" 
