BEGIN { FS = "," } ; 
{if ($6 ~ /"[0-9"]+"/) print $6; 
else if ($7 ~ /"[0-9]+"/ ) print $7;
else print $0;

	}

