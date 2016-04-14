package helpers

import (
	"bytes"
	"os/exec"
	"strings"
)

type DigAnswer struct {
	Host    string
	Address string
}

type DigResult struct {
	Answers []DigAnswer
}

func Dig(host string, ip string) (DigResult, error) {
	var digOut bytes.Buffer
	retval := DigResult{}
	retval.Answers = make([]DigAnswer, 0)

	cmdDig := exec.Command("dig", "@"+ip, host)
	cmdDig.Stdout = &digOut

	err := cmdDig.Run()
	if err != nil {
		return retval, err
	}

	aryDigResults := strings.Split(digOut.String(), "\n")

	for _, digResult := range aryDigResults {
		if strings.Contains(digResult, host) && !strings.Contains(digResult, ";") {
			aryDigResult := strings.Split(strings.Replace(digResult, "\t", " ", -1), " ")
			myHost := strings.Split(digResult, " ")[0]
			myAddress := aryDigResult[len(aryDigResult)-1]
			retval.Answers = append(retval.Answers, DigAnswer{Host: myHost, Address: myAddress})
		}
	}
	return retval, nil
}

func main() {
	asdf, _ := Dig("consul.service.rdpgjrb.consul", "10.244.2.2")
	for _, res := range asdf.Answers {
		println(res.Host)
	}
}
