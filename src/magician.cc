#include "magician.h"

Magician::Magician(uv_loop_t* loop=NULL)
    :loop_(loop)
     db_(NULL)

{
}

bool Magician::Open(std::string db_path)
{

}

const google::protobuf::Message& Magician::GetModel(const google::protobuf::Descriptor* descriptor, const std::string& id, int depth=0)
{
    CHECK(descriptor->file()->pool() == DescriptorPool::generated_pool());

    const google::protobuf::Message* prototype = NULL;
    prototype = google::protobuf::MessageFactory::generated_factory()->GetPrototype(descriptor);
    google::protobuf::Message* model = NULL;

    std::string key = descriptor->full_name() + "_" + id;
    std::map<std::string, ::google::protobuf::Message*>::iterator it;
    it = cache_rw_.find(key);
    if (cache_rw_.end() != it) {
        return *it->second;
    }

    it = cache_ro_.find(key);
    if (cache_ro_.end() != it) {
        return *it->second;
    }

    std::string value;
    leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &value);
    if (!status.ok()) {
        return *prototype;
    }

    try {
        model = prototype->New();
        model->ParseFromString(value);
    } catch (std::bad_alloc) {
        delete model;
        return *prototype;
    }

    cache_ro_[key] = model;
    return *model;
}

google::protobuf::Message* Megician::MutableModel(const google::protobuf::Descriptor* descriptor, const std::string& id, int depth, google::protobuf::Message* initial)
{
    CHECK(descriptor->file()->pool() == DescriptorPool::generated_pool());

    const google::protobuf::Message* prototype = NULL;
    prototype = google::protobuf::MessageFactory::generated_factory()->GetPrototype(descriptor);
    google::protobuf::Message* model = NULL;

    std::string key = descriptor->full_name() + "_" + id;
    std::map<std::string, ::google::protobuf::Message*>::iterator it;
    it = cache_rw_.find(key);
    if (cache_rw_.end() != it) {
        return google::protobuf::down_cast<Model*>(it->second);
    }

    it = cache_ro_.find(key);
    if (cache_ro_.end() != it) {
        model = google::protobuf::down_cast<Model*>(it->second);
        cache_rw_[key] = model;
        cache_ro_.erase(it);
        return model;
    }

    std::string value;
    leveldb::Status status = db->Get(leveldb::ReadOptions(), key, &value);
    if (!status.ok() && initial == NULL) {
        return NULL;
    }

    try {
        model = prototype->New();
        if (status.ok()) {
            model->ParseFromString(value);
        } else {
            model->CopyFrom(initial);
        }
    } catch (std::bad_alloc) {
        delete model;
        return NULL;
    }

    cache_rw_[key] = model;

    return model;
}
