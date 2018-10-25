
let log = Log.from "lwt_gauge"

type stream = {
  name : string option;
}

type gauge = {
  mutable streams : stream Dllist.node_t option;
}

let gauge g s stream =
  let handle =
    match g.streams with
    | Some streams -> Dllist.prepend streams s
    | None ->
    let streams = Dllist.create s in
    g.streams <- Some streams;
    streams
  in
  let remove () =
    match g.streams with
    | None -> assert false
    | Some streams when streams != handle -> Dllist.remove handle
    | Some _ ->
    let next = Dllist.drop handle in
    g.streams <- if next != handle then Some next else None
  in
  let fin = Lwt_stream.from_direct (fun () -> remove (); None) in
  Lwt_stream.append stream fin

let end_gauge { streams; } =
  match streams with
  | None -> ()
  | Some streams ->
  let streams = Dllist.to_list streams in
  log #warn "%d streams are still active:" (List.length streams);
  List.iteri begin fun i { name; } ->
    log #warn "#%d %s is still active" i (Option.default "<unnamed>" name)
  end streams

module Lwt_stream = struct

  include Lwt_stream

  let tls = Lwt.new_key ()

  let from ?name f =
    match Lwt.get tls with
    | None -> from f
    | Some g -> gauge g { name; } (from f)

  let from_direct ?name f =
    match Lwt.get tls with
    | None -> from_direct f
    | Some g -> gauge g { name; } (from_direct f)

  let create ?name () =
    match Lwt.get tls with
    | None -> create ()
    | Some g ->
    let (s, f) = create () in
    gauge g { name; } s, f

  let create_with_reference ?name () =
    match Lwt.get tls with
    | None -> create_with_reference ()
    | Some g ->
    let (s, f, r) = create_with_reference () in
    gauge g { name; } s, f, r

  let create_bounded ?name n =
    match Lwt.get tls with
    | None -> create_bounded n
    | Some g ->
    let (s, p) = create_bounded n in
    gauge g { name; } s, p

  let of_list ?name l =
    match Lwt.get tls with
    | None -> of_list l
    | Some g -> gauge g { name; } (of_list l)

  let of_array ?name a =
    match Lwt.get tls with
    | None -> of_array a
    | Some g -> gauge g { name; } (of_array a)

  let of_string ?name s =
    match Lwt.get tls with
    | None -> of_string s
    | Some g -> gauge g { name; } (of_string s)

  let clone ?name s =
    match Lwt.get tls with
    | None -> clone s
    | Some g -> gauge g { name; } (clone s)

  let with_gauge ~enable f =
    match enable with
    | false -> f
    | true ->
    let gauge = { streams = None; } in
    Lwt.with_value tls (Some gauge) @@ fun () ->
    (f) [%lwt.finally end_gauge gauge; Lwt.return_unit; ]

end
