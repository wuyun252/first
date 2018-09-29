package dnslink

import (
	"time"
	"common"
)

type ResInfoResponseParam struct {
	GetGuardLevel   int64   `json:"geo_guard_level"`
	LoadGuardLevel  int64   `json:"load_guard_level"`
	LoadGuardRatio  float64 `json:"load_guard_ratio"`
	LowLoadProbStep float64 `json:"low_load_prob_step"`
	LoadLoadRatio   float64 `json:"low_load_ratio"`
	MaxProb         float64 `json:"max_prob"`
	MaxThrowNum     int64   `json:"max_throw_num"`
	MinThrowNum     int64   `json:"min_throw_num"`
	ProbDownMaxFlow int64   `json:"prob_down_max_flow"`
	ProbDownStep    float64 `json:"prob_down_step"`
	ProbUpmaxFlow   int64   `json:"prob_up_max_flow"`
	ProbUpStep      float64 `json:"prob_up_step"`
}

type ResInfoResponsePlat struct {
	Id   int64  `json:"id"`
	Name string `json:"name"`
}

type ResInfoResponseData struct {
	BackZone       string                `json:"back_zone"`
	BackZonId      int64                 `json:"back_zone_id"`
	GeoGuardLevel  int64                 `json:"geo_guard_level"`
	Id             int64                 `json:"id"`
	LoadGuardLevel int64                 `json:"load_guard_level"`
	LoadGuardRatio float64               `json:"load_guard_ratio"`
	MainOper       string                `json:"mainoper"`
	Params         ResInfoResponseParam  `json:"param"`
	Plats          []ResInfoResponsePlat `json:"plats"`
	ResId          int64                 `json:"res_id"`
	Zone           string                `json:"zone"`
}

type ResInfoResponse struct {
	Errno int64                 `json:"errno"`
	Error string                `json:"error"`
	Seq   int64                 `json:"seq"`
	Data  []ResInfoResponseData `json:"data, omitempty"`
}

func (res *ResInfoResponse) GetErrno() int64 {
	return res.Errno
}

func (res *ResInfoResponse) GetError() string {
	return res.Error
}

// get all res_id from cgi
func getResIds(url string, timeout time.Duration) (map[int64]bool, error) {
	var respJson ResInfoResponse
	err := common.GetJsonResponseFromCgi(url, timeout, &respJson)
	if err != nil {
		return nil, err
	}
	result := make(map[int64]bool)
	for _, resData := range respJson.Data {
		result[resData.ResId] = true
	}
	return result, nil
}
