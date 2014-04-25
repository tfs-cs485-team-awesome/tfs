#!/bin/bash
#Starts a bunch of TFS's, each with its own directory.
#Args: #clients #chunkservers
runInNewWindow()
{
echo "$1"
osascript <<END 
tell application "Terminal"
	do script "cd \"`pwd`\";$1"
end tell
END
}
cd dist
for i in `seq 1 $1`; do
	mkdir test$i
	cp TFS.jar test$i
done
for i in `seq 1 $2`; do
	mkdir chunk$i
	cp TFS.jar chunk$i
done
#tmux new-session -d -s foo 'java -jar TFS.jar server 6789'
runInNewWindow "java -jar TFS.jar server 6789 | tee log.txt"
for i in `seq 1 $1`; do
	cd test$i
	#tmux split-window -h 'java -jar TFS.jar client localhost:6789'
	runInNewWindow "java -jar TFS.jar test localhost:6789 | tee log.txt"
	cd ..
	cp README.TXT test$i
done   
for i in `seq 1 $2`; do
	cd chunk$i
	portnum=`expr $i + 6800`
	echo $portnum
	#tmux split-window -v 'java -jar TFS.jar chunk localhost:6789 '"$portnum"
	runInNewWindow "java -jar TFS.jar chunk localhost:6789 "$portnum" | tee log.txt"
	cd ..
done
#tmux attach-session -d
