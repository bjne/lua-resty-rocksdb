package.path = './lib/?.lua;;'

local rocksdb = require "resty.rocksdb"

local db, err = rocksdb.open('/tmp/foox', {create_if_missing = 1})

if not db then
    return nil, print(err)
end

local ok, err

local niter = 1000000

for i=1,niter do
    ok, err = db:put('foo'..tostring(i), 'bar'..tostring(i))
    if not ok then
        return nil, print(err)
    end
end

for i=1,niter do
    ok, err = db:get('foo'..tostring(i))
    if not ok then
        return nil, print(err)
    end
end

local secondary, err = rocksdb.open_secondary('/tmp/foox', '/tmp/fooy')
if not secondary then
    return nil, print(err)
end

for i=1,niter do
    ok, err = secondary:get('foo'..tostring(i))
    if not ok then
        return nil, print(err)
    end

    print(ok)
end
