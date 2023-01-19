local ffi = require "ffi"
local common = require "rocksdb.common"
local options = require "rocksdb.options"

local ffi_string = ffi.string

ffi.cdef[[
extern char** rocksdb_list_column_families(
    const rocksdb_options_t* options, const char* name, size_t* lencf,
    char** errptr);

/* api */
extern rocksdb_t* rocksdb_open(
    const rocksdb_options_t* options, const char* name, char** errptr);

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

    rocksdb = function(fn, ...)
        local err_p = select(select('#', ...), ...)
        err_p[0] = NULL
        local ok, ret = pcall(fn, ...)
        if ok then
            return err_p[0] == NULL and (ret or true), err_p[0] ~= NULL and ffi_string(err_p[0]) or nil
        end

        return nil, ret
    end
end

local open, open_secondary do
    local size_p = ffi.new('size_t[1]')
    local err_p = ffi.new('char *[1]')

    local secondary_opt = {max_open_files = -1, create_if_missing = false }

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

        if self.secondary then
            self:try_catch_up()
        end

        return self
    end

    open = function(name, opt, read_opt, write_opt)
        return _open(lib.rocksdb_open, name, opt, read_opt, write_opt, err_p)
    end

    open_secondary = function(name, secondary_name, opt, read_opt)
        return _open(lib.rocksdb_open_as_secondary, name, opt, read_opt, nil, secondary_name, err_p)
    end
end

--local db, err = open('/tmp/foox', {create_if_missing = 1, compression = lib.rocksdb_snappy_compression})
--local db, err = open('/tmp/foox', {create_if_missing = 1})
local db, err = open_secondary('/tmp/foox', '/tmp/fooy', {create_if_missing = 1})

print(db:try_catch_up())

if not db then
    return nil, print(err)
end

local ok, err

--for i=1,1000 do
--    ok, err = db:put('foo', 'bar')
--    if not ok then
--        return nil, print(err)
--    end
--end

for i=1,1 do
    ok, err = db:get('foo')
    if not ok then
        return nil, print(err)
    end
    print(ok)
end
