PORT=6380

redis-cli -p $PORT REPLCONF GETACK "*"
redis-cli -p $PORT PING
redis-cli -p $PORT REPLCONF GETACK "*"

echo -e "\t Replicating..."

redis-cli SET apple banana
redis-cli SET apple pineapple
redis-cli -p $PORT REPLCONF GETACK "*"
echo -e "\tShould be 163 here."

redis-cli -p $PORT GET apple
echo -e "\tShould be pineapple here."