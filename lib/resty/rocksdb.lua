local ffi = require "ffi"
local common = require "resty.rocksdb.common"
local options = require "resty.rocksdb.options"

local ffi_gc = ffi.gc
local ffi_string = ffi.string

local _M = { _VERSION = 0.1 }

ffi.cdef[[
extern char** rocksdb_list_column_families(
    const rocksdb_options_t* options, const char* name, size_t* lencf,
    char** errptr);

/* api */
extern rocksdb_t* rocksdb_open(
    const rocksdb_options_t* options, const char* name, char** errptr);

extern void rocksdb_close(rocksdb_t* db);

extern rocksdb_t* rocksdb_open_as_secondary(
    const rocksdb_options_t* options, const char* name,
    const char* secondary_path, char** errptr);

extern void rocksdb_try_catch_up_with_primary(
    rocksdb_t* db, char** errptr);

extern void rocksdb_put(
    rocksdb_t* db, const rocksdb_writeoptions_t* options, const char* key,
    size_t keylen, const char* val, size_t vallen, char** errptr);

extern char* rocksdb_get(
    rocksdb_t* db, const rocksdb_readoptions_t* options, const char* key,
    size_t keylen, size_t* vallen, char** errptr);
]]

local lib = ffi.load('rocksdb')

local rocksdb do
    local NULL = ffi.new('void *')

    rocksdb = function(fn, db, ...)
        if not db then
            return nil, "database closed"
        end

        local err_p = select(select('#', ...), ...)
        err_p[0] = NULL
        local ok, ret = pcall(fn, db, ...)
        if ok then
            return err_p[0] == NULL and (ret or true), err_p[0] ~= NULL and ffi_string(err_p[0]) or nil
        end

        return nil, ret
    end
end

local size_p = ffi.new('size_t[1]')
local err_p = ffi.new('char *[1]')

local secondary_opt = {max_open_files = -1, create_if_missing = false}

local mt = {__index = {
    put = function(self, k, v, wo)
        wo = wo and options.write_options(wo) or self.write_options

        return rocksdb(lib.rocksdb_put, self.db, wo, k, #k, v, #v, err_p)
    end,
    get = function(self, k, ro)
        ro = ro and options.read_options(ro) or self.read_options
        v, err = rocksdb(lib.rocksdb_get, self.db, ro, k, #k, size_p, err_p)

        return v and ffi_string(v, size_p[0]), err
    end,
    try_catch_up = function(self)
        return rocksdb(lib.rocksdb_try_catch_up_with_primary, self.db, err_p)
    end,
    close = function(self)
        lib.rocksdb_close(ffi_gc(self.db, nil))
        self.db = nil
    end
}}

-- local ok, r = lib.rocksdb_list_column_families(opts, name, size_p, err_p)

local _open = function(fn, name, opt, read_opt, write_opt, ...)
    local self, err = setmetatable({
        secondary = select('#', ...) == 2,
        read_options = options.read_options(read_opt)
    }, mt)

    opt = options.options(opt)

    if self.secondary then
        opt:set(secondary_opt)
    else
        self.write_options = options.write_options(write_opt)
    end

    self.db, err = rocksdb(fn, opt, name, ...)

    if not self.db then
        return nil, err
    end

    ffi_gc(self.db, lib.rocksdb_close)

    if self.secondary then
        self:try_catch_up()
    end

    return self
end

_M.open = function(name, opt, read_opt, write_opt)
    return _open(lib.rocksdb_open, name, opt, read_opt, write_opt, err_p)
end

_M.open_secondary = function(name, secondary_name, opt, read_opt)
    return _open(lib.rocksdb_open_as_secondary, name, opt, read_opt, nil, secondary_name, err_p)
end

return _M
