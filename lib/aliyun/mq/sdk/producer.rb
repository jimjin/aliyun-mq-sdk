module Aliyun::Mq::Sdk
  class Producer
    include HTTParty

    DEFAULT_BASE_URI = 'http://publictest-rest.ons.aliyun.com/message/'

    attr_accessor :access_key, :secret_key, :producer_id, :region_url, :default_topic, :topic

    def initialize(access_key, secret_key, producer_id, opts={})
      @access_key = access_key
      @secret_key = secret_key
      @producer_id = producer_id

      @region_url = opts[:region_url] || DEFAULT_BASE_URI
      @default_topic = opts[:default_topic]
    end

    def headers(msg, time)
      sign = Auth.post_sign(secret_key, topic, producer_id, msg, time)
      {"Signature" => sign, "AccessKey" => access_key, "ProducerID" => producer_id, "Content-Type" => 'text/html;charset=UTF-8'}
    end

    def send(msg, opts={})
      @time = opts[:time] || (Time.now.to_f * 1000).to_i
      @topic = opts[:topic] || default_topic
      tag = opts[:tag]
      key = opts[:key]
      is_order = opts[:is_order]
      sharding_key = opts[:sharding_key]

      hds = headers(msg, @time)

      query = {"topic" => topic, "time" => @time}

      query["tag"] = tag if tag
      query["key"] = key if key

      if is_order && !sharding_key.nil?
        hds = hds.merge("isOrder" => is_order.to_s, "shardingKey" => sharding_key)
      end
      res = self.class.post(region_url, headers: hds, query: query, body: msg)
      if res.parsed_response
        rslt = Utils.symbolize_keys(JSON.parse(res.parsed_response).merge(success: true))
      else
        rslt = {success: false, msg: res.response}
      end
      rslt
    end
  end
end