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
	mkdir client$i
	cp TFS.jar client$i
done
for i in `seq 1 $2`; do
	mkdir chunk$i
	cp TFS.jar chunk$i
done
#tmux new-session -d -s foo 'java -jar TFS.jar server 6789'
runInNewWindow "java -jar TFS.jar server 6789"
for i in `seq 1 $1`; do
	cd client$i
	#tmux split-window -h 'java -jar TFS.jar client localhost:6789'
	runInNewWindow "java -jar TFS.jar client localhost:6789"
	cd ..
done   
for i in `seq 1 $2`; do
	cd chunk$i
	portnum=`expr $i + 6800`
	echo $portnum
	#tmux split-window -v 'java -jar TFS.jar chunk localhost:6789 '"$portnum"
	runInNewWindow "java -jar TFS.jar chunk localhost:6789 "$portnum""
	cd ..
done
#tmux attach-session -d
