require 'spec_helper'

describe Aliyun::Mq::Sdk do
  before(:each) do
    @access_key = Config::AK
    @secret_key = Config::SK
    @producer_1 = Config::PRODUCER_ID_1
    @consumer_1 = Config::CONSUMER_ID_1
    @topic_1 = Config::TOPIC_1
  end

  it 'has a version number' do
    expect(Aliyun::Mq::Sdk::VERSION).not_to be nil
  end

  it 'producer send order' do
    producer = Aliyun::Mq::Sdk::Producer.new(@access_key, @secret_key, @producer_1)
    r = producer.send('test order msg', topic: @topic_1, is_order: true, sharding_key: 'print')
    expect(r[:success]).to be true
    expect(r[:msgId]).not_to be nil
    expect(r[:sendStatus]) == 'SEND_OK'
  end

  it 'consumer receive msg' do
    consumer = Aliyun::Mq::Sdk::Consumer.new(@access_key, @secret_key, @consumer_1)
    r = consumer.receive(topic: @topic_1)
    # [{“body”:”HelloMQ”, ”bornTime”:”1418973464204”, “msgHandle”:”X1BFTkRJTkdNU0dfXyVSRVRSWSUkbG9uZ2ppJENJRF9sb25namlfdGxvbmdqaQ==”, “msgId”:”0A021F7300002A9F000000000647076D”, ”reconsumeTimes”:1}]
    expect(r[:items].size) != 0
    expect(r[:success]).to be true
    expect(r[:items][0][:body]).not_to be nil
    expect(r[:items][0][:bornTime]).not_to be nil
    expect(r[:items][0][:msgHandle]).not_to be nil
    expect(r[:items][0][:msgId]).not_to be nil
    expect(r[:items][0][:reconsumeTimes]).to be 1
  end
end
