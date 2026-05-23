bash scripts/reset.sh
source scripts/env/bin/activate
python3 ./scripts/simple_producer.py --interval 0 --count 10
python3 ./scripts/simple_consumer.py --timeout 1
python3 scripts/performance_test.py