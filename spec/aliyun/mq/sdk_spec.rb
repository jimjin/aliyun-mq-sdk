require 'spec_helper'

describe Aliyun::Mq::Sdk do
  before(:each) do
    @access_key = Config::AK
    @secret_key = Config::SK
    @producer_1 = Config::PRODUCER_ID_1
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
end
